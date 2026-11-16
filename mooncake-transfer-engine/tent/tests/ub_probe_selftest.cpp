// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Two-node probe-only selftest. It bypasses TransferEngine/bootstrap:
//
//   node A: discover probe devices -> create local resources -> write a.json
//           -> wait for b.json -> build remote edges -> run PLM + scheduler
//   node B: same, mirrored.
//
// Run both nodes against a shared exchange directory (NFS/sshfs) or copy the
// JSON files manually. The probe test mode is enabled by default; use
// --apply-health=false for a pure link check.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/probe_link_manager.h"
#include "tent/transport/ub/probe_peer_handshake.h"
#include "tent/transport/ub/probe_poster_urma.h"
#include "tent/transport/ub/probe_provider_standalone.h"
#include "tent/transport/ub/probe_scheduler.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {
namespace {

std::atomic<bool> g_stop{false};

void onSignal(int) { g_stop.store(true, std::memory_order_release); }

uint64_t selftestNowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct Options {
    std::string node;
    std::string peer;
    std::string exchange_dir{"/tmp/tent_probe_selftest"};
    uint64_t duration_s{60};
    uint64_t wait_timeout_s{120};
    uint64_t test_interval_ms{5000};
    uint64_t interval_ms{1000};
    uint64_t timeout_ms{1000};
    uint32_t jfc_depth{64};
    uint32_t peer_budget{4};
    uint64_t source_page_size{0};
    bool apply_health{false};
    std::vector<std::string> device_filter;
};

void usage(const char* argv0) {
    std::cout
        << "Usage: " << argv0 << " --node <name> --peer <name> [options]\n"
        << "  --exchange-dir <dir>      shared directory for <node>.json "
           "(default /tmp/tent_probe_selftest)\n"
        << "  --duration <sec>          run time (default 60)\n"
        << "  --wait-timeout <sec>      wait for peer JSON (default 120)\n"
        << "  --test-interval-ms <ms>  probe test cadence (default 5000)\n"
        << "  --apply-health <bool>     false = pure link check (default "
           "false)\n"
        << "  --interval-ms <ms>        normal retry interval (default 1000)\n"
        << "  --timeout-ms <ms>         probe completion timeout (default "
           "1000)\n"
        << "  --jfc-depth <n>           probe JFC depth (default 64)\n"
        << "  --peer-budget <n>         per-peer in-flight budget (default "
           "4)\n"
        << "  --device-filter <a,b>     native/topology names (default all "
           "non-bonding)\n"
        << "  --source-page-size <n>    0 = system page (default 0)\n"
        << "\nRemove stale <node>.json files from the exchange directory "
           "before "
           "a new run.\n";
}

bool parseBool(const std::string& value, bool& out) {
    if (value == "1" || value == "true" || value == "yes") {
        out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "no") {
        out = false;
        return true;
    }
    return false;
}

bool parseOptions(int argc, char** argv, Options& options) {
    for (int i = 1; i < argc; ++i) {
        const std::string key = argv[i];
        auto value = [&](std::string& out) {
            if (i + 1 >= argc) return false;
            out = argv[++i];
            return true;
        };
        if (key == "--help" || key == "-h") {
            return false;
        } else if (key == "--node") {
            if (!value(options.node)) return false;
        } else if (key == "--peer") {
            if (!value(options.peer)) return false;
        } else if (key == "--exchange-dir") {
            if (!value(options.exchange_dir)) return false;
        } else if (key == "--duration") {
            std::string text;
            if (!value(text)) return false;
            options.duration_s = std::stoull(text);
        } else if (key == "--wait-timeout") {
            std::string text;
            if (!value(text)) return false;
            options.wait_timeout_s = std::stoull(text);
        } else if (key == "--test-interval-ms") {
            std::string text;
            if (!value(text)) return false;
            options.test_interval_ms = std::stoull(text);
        } else if (key == "--interval-ms") {
            std::string text;
            if (!value(text)) return false;
            options.interval_ms = std::stoull(text);
        } else if (key == "--timeout-ms") {
            std::string text;
            if (!value(text)) return false;
            options.timeout_ms = std::stoull(text);
        } else if (key == "--jfc-depth") {
            std::string text;
            if (!value(text)) return false;
            options.jfc_depth = static_cast<uint32_t>(std::stoul(text));
        } else if (key == "--peer-budget") {
            std::string text;
            if (!value(text)) return false;
            options.peer_budget = static_cast<uint32_t>(std::stoul(text));
        } else if (key == "--source-page-size") {
            std::string text;
            if (!value(text)) return false;
            options.source_page_size = std::stoull(text);
        } else if (key == "--apply-health") {
            std::string text;
            if (!value(text) || !parseBool(text, options.apply_health)) {
                return false;
            }
        } else if (key == "--device-filter") {
            std::string text;
            if (!value(text)) return false;
            size_t start = 0;
            while (start <= text.size()) {
                const size_t comma = text.find(',', start);
                const std::string item = text.substr(
                    start, comma == std::string::npos ? std::string::npos
                                                      : comma - start);
                if (!item.empty()) options.device_filter.push_back(item);
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        } else {
            std::cerr << "unknown option: " << key << "\n";
            return false;
        }
    }
    if (options.node.empty() || options.peer.empty()) return false;
    if (options.node == options.peer) {
        std::cerr << "--node and --peer must differ\n";
        return false;
    }
    return true;
}

nlohmann::json descriptorToJson(const SegmentDescriptor& descriptor) {
    return nlohmann::json{{"schema_version", descriptor.schema_version},
                          {"urma_api_version", descriptor.urma_api_version},
                          {"urma_abi_size", descriptor.urma_abi_size},
                          {"hex", descriptor.hex}};
}

bool descriptorFromJson(const nlohmann::json& value,
                        SegmentDescriptor& descriptor) {
    if (!value.is_object()) return false;
    descriptor.schema_version = value.value("schema_version", 0U);
    descriptor.urma_api_version = value.value("urma_api_version", 0U);
    descriptor.urma_abi_size = value.value("urma_abi_size", 0U);
    descriptor.hex = value.value("hex", std::string());
    return descriptor.schema_version != 0 && !descriptor.hex.empty();
}

struct PeerPort {
    int32_t local_nic{-1};
    std::string topology_name;
    std::string device_eid;
    SegmentDescriptor area_descriptor;
    uint64_t area_epoch{0};
    uint32_t area_token{0};
    uint32_t jetty_id{0};
    uint32_t jetty_uasid{0};
    uint32_t jetty_token{0};
};

class StaticPeerSource final : public ProbePeerDescriptorSource {
   public:
    explicit StaticPeerSource(std::vector<ProbePeerDescriptor> descriptors)
        : descriptors_(std::move(descriptors)) {}

    Status snapshot(std::vector<ProbePeerDescriptor>& descriptors) override {
        descriptors = descriptors_;
        return Status::OK();
    }

   private:
    std::vector<ProbePeerDescriptor> descriptors_;
};

void writeLocalJson(const std::string& path, const std::string& node,
                    const StandaloneProbeResourceProvider& provider) {
    const auto ports = provider.portViews();
    const auto areas = provider.localAreaViews();
    std::map<Topology::NicID,
             const StandaloneProbeResourceProvider::LocalAreaView*>
        area_by_nic;
    for (const auto& area : areas) area_by_nic[area.local_nic] = &area;

    nlohmann::json root;
    root["schema"] = 1;
    root["node"] = node;
    root["ports"] = nlohmann::json::array();
    for (const auto& port : ports) {
        auto area = area_by_nic.find(port.local_nic);
        if (area == area_by_nic.end()) continue;
        const auto& local_area = *area->second;
        root["ports"].push_back(nlohmann::json{
            {"local_nic", port.local_nic},
            {"topology_name", port.topology_name},
            {"device_eid", port.device_eid},
            {"area_descriptor", descriptorToJson(local_area.descriptor)},
            {"area_length", 1},
            {"area_page_size", local_area.length},
            {"area_epoch", local_area.epoch},
            {"area_token", local_area.token},
            {"probe_jetty",
             {{"eid", port.device_eid},
              {"id", port.native_jetty_id},
              {"uasid", port.native_jetty_uasid},
              {"token", local_area.token}}}});
    }

    std::filesystem::create_directories(
        std::filesystem::path(path).parent_path());
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) throw std::runtime_error("cannot write " + tmp);
        out << root.dump(2) << "\n";
    }
    std::filesystem::rename(tmp, path);
}

bool waitForPeerJson(const std::string& path, uint64_t timeout_s,
                     nlohmann::json& root) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
    while (!g_stop.load(std::memory_order_acquire)) {
        if (std::filesystem::exists(path)) {
            try {
                std::ifstream in(path);
                in >> root;
                if (root.value("schema", 0) == 1) return true;
            } catch (const std::exception& error) {
                std::cerr << "peer JSON is not ready: " << error.what() << "\n";
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::vector<PeerPort> parsePeerPorts(const nlohmann::json& root,
                                     const std::string& peer_node) {
    std::vector<PeerPort> ports;
    if (!root.contains("node") ||
        root["node"].get<std::string>() != peer_node) {
        throw std::runtime_error("peer JSON node does not match --peer");
    }
    if (!root.contains("ports") || !root["ports"].is_array()) {
        throw std::runtime_error("peer JSON has no ports array");
    }
    for (const auto& item : root["ports"]) {
        PeerPort port;
        port.local_nic = item.value("local_nic", -1);
        port.topology_name = item.value("topology_name", std::string());
        port.device_eid = item.value("device_eid", std::string());
        port.area_epoch = item.value("area_epoch", 0ULL);
        port.area_token = item.value("area_token", 0U);
        if (!item.contains("area_descriptor") ||
            !descriptorFromJson(item["area_descriptor"],
                                port.area_descriptor) ||
            port.local_nic < 0 || port.area_epoch == 0 ||
            port.device_eid.empty()) {
            throw std::runtime_error("peer JSON has an invalid port");
        }
        if (item.contains("probe_jetty")) {
            const auto& jetty = item["probe_jetty"];
            port.jetty_id = jetty.value("id", 0U);
            port.jetty_uasid = jetty.value("uasid", 0U);
            port.jetty_token = jetty.value("token", port.area_token);
        }
        if (port.jetty_id == 0) {
            throw std::runtime_error("peer JSON has no probe Jetty id");
        }
        ports.push_back(std::move(port));
    }
    if (ports.empty()) throw std::runtime_error("peer JSON has no ports");
    return ports;
}

void printStats(const ProbeLinkManager& plm, uint64_t elapsed_s) {
    const auto stats = plm.stats();
    size_t healthy = 0;
    size_t isolated = 0;
    size_t recovering = 0;
    for (const auto& entry : plm.healthView()) {
        if (entry.health == LinkHealth::kHealthy) ++healthy;
        if (entry.health == LinkHealth::kIsolated) ++isolated;
        if (entry.health == LinkHealth::kRecovering) ++recovering;
    }
    std::cout << "[probe] t=" << elapsed_s
              << "s inflight=" << plm.inflightCount()
              << " test_post=" << stats.probe_test_post
              << " ok=" << stats.probe_test_ok
              << " fail=" << stats.probe_test_fail
              << " missed=" << stats.probe_test_missed
              << " timeout=" << stats.probe_timeout_unknown
              << " healthy=" << healthy << " isolated=" << isolated
              << " recovering=" << recovering << std::endl;
}

}  // namespace

int runSelftest(const Options& options) {
    auto adapter = createDefaultUrmaAdapter();
    if (!adapter || !adapter->available()) {
        std::cerr
            << "TENT UB adapter is unavailable: rebuild with USE_UB=ON and a "
               "real URMA library (URMA_LIBRARY)\n";
        return 1;
    }
    auto status = adapter->initialize();
    if (!status.ok()) {
        std::cerr << "adapter initialize failed: " << status.ToString() << "\n";
        return 1;
    }

    ProbeParams params;
    params.enabled = true;
    params.provider = ProbeProviderKind::kStandalone;
    params.jfc_mode = ProbeJfcMode::kDedicated;
    params.jfc_depth = options.jfc_depth;
    params.interval_ms = options.interval_ms;
    params.timeout_ms = options.timeout_ms;
    params.peer_budget = options.peer_budget;
    params.device_filter = options.device_filter;
    params.source_page_size = options.source_page_size;
    params.test_periodic_all = true;
    params.test_interval_ms = options.test_interval_ms;
    params.test_apply_health = options.apply_health;
    if (!params.valid()) {
        std::cerr << "invalid probe parameters\n";
        return 1;
    }

    auto provider =
        std::make_unique<StandaloneProbeResourceProvider>(adapter, params);
    status = provider->discover();
    if (!status.ok()) {
        std::cerr << "probe discovery failed: " << status.ToString() << "\n";
        return 1;
    }
    status = provider->prepareLocal();
    if (!status.ok()) {
        std::cerr << "probe local resource creation failed: "
                  << status.ToString() << "\n";
        return 1;
    }

    const std::filesystem::path exchange(options.exchange_dir);
    const auto local_file = exchange / (options.node + ".json");
    const auto peer_file = exchange / (options.peer + ".json");
    try {
        writeLocalJson(local_file.string(), options.node, *provider);
    } catch (const std::exception& error) {
        std::cerr << "cannot write local JSON: " << error.what() << "\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    std::cout << "local descriptors written to " << local_file
              << "; waiting for " << peer_file << std::endl;

    nlohmann::json peer_json;
    if (!waitForPeerJson(peer_file.string(), options.wait_timeout_s,
                         peer_json)) {
        std::cerr << "timed out waiting for peer JSON\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    std::vector<PeerPort> peer_ports;
    try {
        peer_ports = parsePeerPorts(peer_json, options.peer);
    } catch (const std::exception& error) {
        std::cerr << "invalid peer JSON: " << error.what() << "\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    const auto local_ports = provider->portViews();
    if (local_ports.empty()) {
        std::cerr << "no local probe port\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    std::map<int32_t, PeerPort> peer_by_nic;
    for (auto& port : peer_ports) peer_by_nic[port.local_nic] = port;

    std::vector<ProbePeerDescriptor> descriptors;
    constexpr SegmentID kPeerSegmentId = 1;
    for (const auto& local : local_ports) {
        auto peer = peer_by_nic.find(local.local_nic);
        if (peer == peer_by_nic.end()) {
            std::cerr << "warning: no peer port for local_nic="
                      << local.local_nic << "\n";
            continue;
        }
        ProbePeerDescriptor descriptor;
        descriptor.local_nic = local.local_nic;
        descriptor.peer_segment_id = kPeerSegmentId;
        descriptor.remote_nic = peer->second.local_nic;
        descriptor.peer_nic_path = "peer@" + options.peer;
        descriptor.topology_epoch = 1;
        descriptor.target_epoch = 1;
        descriptor.area_descriptor = peer->second.area_descriptor;
        descriptor.area_length = 1;
        descriptor.area_epoch = peer->second.area_epoch;
        descriptor.area_token = peer->second.area_token;
        descriptor.remote_jetty =
            RemoteJettyInfo{peer->second.device_eid, peer->second.jetty_id,
                            peer->second.jetty_uasid, peer->second.jetty_token};
        descriptor.node_gate_ok = true;
        descriptor.port_gate_ok = true;
        descriptor.profile_gate_ok = true;
        descriptors.push_back(std::move(descriptor));
    }
    if (descriptors.empty()) {
        std::cerr << "no matching local/peer probe port\n";
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    StaticPeerSource source(std::move(descriptors));
    HandshakeProbePeerDirectory directory(adapter, provider->contexts(),
                                          &source);
    provider->setRemoteDirectory(&directory);

    ProbeGraph graph;
    status = provider->snapshot(graph);
    if (!status.ok()) {
        std::cerr << "probe graph build failed: " << status.ToString() << "\n";
        directory.shutdown();
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }

    ProbeLinkManager plm(params);
    plm.setReportSink(provider.get());
    status = plm.initialize(graph);
    if (!status.ok()) {
        std::cerr << "probe manager initialize failed: " << status.ToString()
                  << "\n";
        directory.shutdown();
        provider->shutdown();
        adapter->shutdown();
        return 1;
    }
    provider->setAvailabilitySink(plm.availabilitySink());
    provider->setCompletionSink([&plm](const Completion& completion) {
        if (!isProbeToken(completion.token)) {
            return Status::InvalidArgument("non-probe completion");
        }
        if (!plm.onProbeCompletion(completion)) {
            return Status::InternalError("probe completion was not accepted");
        }
        return Status::OK();
    });

    std::atomic<bool> poll_stop{false};
    std::vector<std::thread> pollers;
    for (const auto& port : graph.ports) {
        if (!port.probe_jfc) continue;
        pollers.emplace_back([&, jfc = port.probe_jfc] {
            while (!poll_stop.load(std::memory_order_acquire)) {
                std::vector<Completion> completions;
                auto poll_status = adapter->poll(jfc, 64, completions);
                if (!poll_status.ok()) {
                    std::cerr
                        << "probe JFC poll failed: " << poll_status.ToString()
                        << "\n";
                    break;
                }
                for (const auto& completion : completions) {
                    (void)plm.onProbeCompletion(completion);
                }
                if (completions.empty()) {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                }
            }
        });
    }

    UrmaProbePoster poster(adapter);
    ProbeScheduler scheduler(plm, poster);
    scheduler.start();

    const uint64_t start_ns = selftestNowNs();
    const uint64_t duration_ns = options.duration_s * 1'000'000'000ULL;
    uint64_t next_report_ns = start_ns + 1'000'000'000ULL;
    std::cout << "probe selftest started: node=" << options.node
              << " peer=" << options.peer
              << " test_interval_ms=" << options.test_interval_ms
              << " apply_health=" << (options.apply_health ? "true" : "false")
              << std::endl;

    while (!g_stop.load(std::memory_order_acquire)) {
        const uint64_t now = selftestNowNs();
        if (now - start_ns >= duration_ns) break;
        if (now >= next_report_ns) {
            printStats(plm, (now - start_ns) / 1'000'000'000ULL);
            next_report_ns += 1'000'000'000ULL;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "stopping probe selftest..." << std::endl;
    const auto drain =
        plm.drainBeforePollStop(selftestNowNs() + params.timeoutNs());
    poll_stop.store(true, std::memory_order_release);
    for (auto& poller : pollers) {
        if (poller.joinable()) poller.join();
    }
    if (drain.unproven != 0) {
        // The design keeps unresolved probe resources alive on the process
        // exit path; do not unimport/close memory that may still be in DMA.
        std::cout << "unproven probe records: " << drain.unproven
                  << "; leaving resources to process teardown" << std::endl;
        std::cout.flush();
        std::cerr.flush();
        std::_Exit(2);
    }
    provider->setAvailabilitySink(nullptr);
    provider->setCompletionSink(nullptr);
    // Unimport remote probe areas before the probe contexts close.
    directory.shutdown();
    provider->shutdown();
    adapter->shutdown();
    std::cout << "probe selftest stopped: settled=" << drain.settled
              << " unproven=" << drain.unproven
              << " drained=" << (drain.drained ? "true" : "false") << std::endl;
    return drain.drained ? 0 : 2;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

int main(int argc, char** argv) {
    using namespace mooncake::tent::ub;
    try {
        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);
        Options options;
        if (!parseOptions(argc, argv, options)) {
            usage(argv[0]);
            return 1;
        }
        return runSelftest(options);
    } catch (const std::exception& error) {
        std::cerr << "probe selftest failed: " << error.what() << "\n";
        return 1;
    }
}
