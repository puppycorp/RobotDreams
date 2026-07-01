use std::cell::{Cell, RefCell};
use std::process::Command;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub(crate) struct CpuSample {
    idle: u64,
    total: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct GpuMetricCache {
    sampled_at: Instant,
    label: String,
}

pub(crate) fn system_memory_label() -> String {
    system_memory_bytes()
        .map(|(used, total)| {
            format!(
                "Memory {} used / {} total",
                format_memory_bytes(used),
                format_memory_bytes(total)
            )
        })
        .unwrap_or_else(|| "Memory --".to_string())
}

fn system_memory_bytes() -> Option<(u64, u64)> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let total = meminfo_value_bytes(&meminfo, "MemTotal:")?;
    let available = meminfo_value_bytes(&meminfo, "MemAvailable:")?;
    Some((total.saturating_sub(available), total))
}

fn meminfo_value_bytes(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let value = line.strip_prefix(key)?;
        let kb = value.split_whitespace().next()?.parse::<u64>().ok()?;
        Some(kb.saturating_mul(1024))
    })
}

fn format_memory_bytes(bytes: u64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * MIB;
    let bytes = bytes as f64;

    if bytes >= GIB {
        format!("{:.1} GB", bytes / GIB)
    } else {
        format!("{:.0} MB", bytes / MIB)
    }
}

pub(crate) fn system_cpu_label(cpu_sample: &Cell<Option<CpuSample>>) -> String {
    let Some(current) = read_cpu_sample() else {
        return "CPU --".to_string();
    };
    let previous = cpu_sample.replace(Some(current));
    let Some(previous) = previous else {
        return "CPU --".to_string();
    };

    let total_delta = current.total.saturating_sub(previous.total);
    if total_delta == 0 {
        return "CPU --".to_string();
    }

    let idle_delta = current.idle.saturating_sub(previous.idle);
    let used_delta = total_delta.saturating_sub(idle_delta);
    let usage = 100.0 * used_delta as f64 / total_delta as f64;
    format!("CPU {:.0}%", usage)
}

pub(crate) fn read_cpu_sample() -> Option<CpuSample> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let cpu_line = stat.lines().next()?.strip_prefix("cpu ")?;
    let values = cpu_line
        .split_whitespace()
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if values.len() < 4 {
        return None;
    }

    let idle = values
        .get(3)
        .copied()
        .unwrap_or(0)
        .saturating_add(values.get(4).copied().unwrap_or(0));
    let total = values.iter().copied().sum();
    Some(CpuSample { idle, total })
}

pub(crate) fn system_gpu_label(cache: &RefCell<Option<GpuMetricCache>>) -> String {
    let now = Instant::now();
    if let Some(cached) = cache.borrow().as_ref()
        && now.duration_since(cached.sampled_at) < Duration::from_secs(2)
    {
        return cached.label.clone();
    }

    let label = read_gpu_usage_percent()
        .map(|usage| format!("GPU {usage}%"))
        .unwrap_or_else(|| "GPU --".to_string());
    *cache.borrow_mut() = Some(GpuMetricCache {
        sampled_at: now,
        label: label.clone(),
    });
    label
}

fn read_gpu_usage_percent() -> Option<u32> {
    let output = Command::new("nvidia-smi")
        .args([
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    stdout.lines().next()?.trim().parse::<u32>().ok()
}
