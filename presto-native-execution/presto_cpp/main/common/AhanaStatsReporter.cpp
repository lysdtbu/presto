/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "AhanaStatsReporter.h"

namespace ahana {

void AhanaStatsReporter::registerMetricExportType(
    folly::StringPiece key,
    facebook::velox::StatType statType) const {
  registerMetricExportType(key.start(), statType);
}

void AhanaStatsReporter::registerMetricExportType(
    const char* key,
    facebook::velox::StatType statType) const {
  VELOX_CHECK(
      statType != facebook::velox::StatType::RATE &&
      statType != facebook::velox::StatType::SUM);
  std::lock_guard<std::mutex> lock(mutex_);
  registeredStats_.emplace(key, statType);
}

const uint64_t getCurrentEpochTimestamp() {
  auto p1 = std::chrono::system_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(p1.time_since_epoch())
      .count();
}

void AhanaStatsReporter::addMetricValue(const char* key, size_t value) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = registeredStats_.find(key);
  VELOX_CHECK(
      it != registeredStats_.end(), "addStatValue() for unregistered stat");
  if (it->second == facebook::velox::StatType::COUNT) {
    auto countItr = counterMetrics.find(key);
    if (countItr == counterMetrics.end()) {
      counterMetrics.emplace(key, 1);
      return;
    }
    countItr->second++;
    return;
  }
  auto statsItr = gaugeMetrics.find(key);
  if (statsItr == gaugeMetrics.end()) {
    gaugeMetrics.emplace(key, std::vector<std::shared_ptr<Gauge>>());
  }
  std::shared_ptr<Gauge> metric = std::make_shared<Gauge>();
  metric->timestamp = getCurrentEpochTimestamp();
  metric->value = value;
  gaugeMetrics[key].push_back(std::move(metric));
}

void AhanaStatsReporter::addMetricValue(folly::StringPiece key, size_t value)
    const {
  addMetricValue(key.start(), value);
}

const std::string AhanaStatsReporter::getMetricsForPrometheus() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::stringstream ss;
  for (const auto gauge : gaugeMetrics) {
    auto metricName = gauge.first.substr(gauge.first.find(".") + 1);
    ss << "# HELP " << metricName << std::endl;
    ss << "# TYPE " << metricName << " gauge" << std::endl;
    for (auto metric : gauge.second) {
      ss << metricName << "{cluster=\"" << cluster_ << "\""
         << ",worker=\"" << workerPod_ << "\"} " << metric->value << " "
         << metric->timestamp * 1000 << std::endl;
    }
  }

  for (const auto counter : counterMetrics) {
    auto metricName = counter.first.substr(counter.first.find(".") + 1);
    ss << "# HELP " << metricName << std::endl;
    ss << "# TYPE " << metricName << " "
       << "counter" << std::endl;
    ss << metricName << "{cluster=\"" << cluster_ << "\",worker=\""
       << workerPod_ << "\"} " << counter.second << std::endl;
  }
  gaugeMetrics.clear();
  counterMetrics.clear();
  return ss.str();
}

// Initialize singleton for the reporter
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new ahana::AhanaStatsReporter();
});
} // namespace ahana
