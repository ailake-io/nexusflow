use serde::Serialize;
use sysinfo::System;

/// CPU/RAM snapshot streamed alongside `ProgressEvent`s on the progress
/// WebSocket (CLAUDE.md §6: "UI Real-Time: WebSockets transmitindo
/// estatísticas de hardware"). No GPU field — `sysinfo` doesn't expose GPU
/// utilization (that's vendor-specific: NVML for NVIDIA, etc.) and nothing
/// in this codebase depends on GPU metrics yet, so it's left out rather
/// than shipped as a permanently-null placeholder.
#[derive(Debug, Clone, Serialize)]
pub struct HardwareStats {
    pub cpu_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
}

/// Wraps a `sysinfo::System` that's refreshed in place across samples —
/// CPU usage is only meaningful as a delta between two refreshes, so a
/// fresh `System` per sample would always report ~0%.
pub struct HardwareMonitor {
    system: System,
}

impl HardwareMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new_all(),
        }
    }

    pub fn sample(&mut self) -> HardwareStats {
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        HardwareStats {
            cpu_percent: self.system.global_cpu_usage(),
            memory_used_bytes: self.system.used_memory(),
            memory_total_bytes: self.system.total_memory(),
        }
    }
}

impl Default for HardwareMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_reports_nonzero_total_memory() {
        let mut monitor = HardwareMonitor::new();
        let stats = monitor.sample();
        assert!(stats.memory_total_bytes > 0);
        assert!(stats.memory_used_bytes <= stats.memory_total_bytes);
    }
}
