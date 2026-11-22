use std::os::unix::process::CommandExt;
use std::process::Command;

#[cfg(target_arch = "x86_64")]
fn optimized_cpu() -> bool {
    // LAHF/SAHF in 64-bit mode is part of the x86-64-v2 baseline.
    let lahf = unsafe { std::arch::x86_64::__cpuid(0x80000001) }.ecx & 1 != 0;
    lahf && std::is_x86_feature_detected!("sse3")
        && std::is_x86_feature_detected!("ssse3")
        && std::is_x86_feature_detected!("sse4.1")
        && std::is_x86_feature_detected!("sse4.2")
        && std::is_x86_feature_detected!("popcnt")
        && std::is_x86_feature_detected!("cmpxchg16b")
        && std::is_x86_feature_detected!("avx")
        && std::is_x86_feature_detected!("avx2")
        && std::is_x86_feature_detected!("bmi1")
        && std::is_x86_feature_detected!("bmi2")
        && std::is_x86_feature_detected!("f16c")
        && std::is_x86_feature_detected!("fma")
        && std::is_x86_feature_detected!("lzcnt")
        && std::is_x86_feature_detected!("movbe")
        && std::is_x86_feature_detected!("aes")
}
#[cfg(not(target_arch = "x86_64"))]
fn optimized_cpu() -> bool {
    false
}
#[cfg(target_arch = "x86_64")]
fn wide_vectors() -> bool {
    std::is_x86_feature_detected!("avx512f")
        && std::is_x86_feature_detected!("avx512bw")
        && std::is_x86_feature_detected!("avx512cd")
        && std::is_x86_feature_detected!("avx512dq")
        && std::is_x86_feature_detected!("avx512vl")
}

#[cfg(not(target_arch = "x86_64"))]
fn wide_vectors() -> bool {
    false
}

#[cfg(target_arch = "x86_64")]
fn cascade_cpu() -> bool {
    use std::arch::x86_64::__cpuid_count;
    let x = unsafe { __cpuid_count(7, 0) };
    let state = unsafe { __cpuid_count(0xd, 1) }.eax;
    let ebx = (1 << 0) | (1 << 10) | (1 << 18) | (1 << 19) | (1 << 23) | (1 << 24);
    let pku = if option_env!("SCORER_CASCADE_PKU") == Some("0") {
        0
    } else {
        (1 << 3) | (1 << 4)
    };
    let ecx = pku | (1 << 11); // AVX512VNNI and any required PKU support.
    wide_vectors()
        && optimized_cpu()
        && std::is_x86_feature_detected!("pclmulqdq")
        && std::is_x86_feature_detected!("rdrand")
        && x.ebx & ebx == ebx
        && x.ecx & ecx == ecx
        && state & 0b1011 == 0b1011
}
#[cfg(not(target_arch = "x86_64"))]
fn cascade_cpu() -> bool {
    false
}

// A longer run can hide the latency of random identity lookups by queuing
// independent events. Small inputs avoid the longer wide-vector pipeline.
fn large_csv_input(events: &std::path::Path, threshold: u64) -> bool {
    let Ok(parts) = std::fs::read_dir(events) else {
        return false;
    };
    let mut bytes = 0u64;
    for part in parts.flatten() {
        if !part.path().extension().is_some_and(|e| e == "csv") {
            continue;
        }
        if let Ok(metadata) = part.metadata() {
            if metadata.is_file() {
                bytes = bytes.saturating_add(metadata.len());
                if bytes >= threshold {
                    return true;
                }
            }
        }
    }
    false
}

fn main() {
    let supported = optimized_cpu();
    let data = std::env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into());
    let requested = std::env::var("PERF_ISA").unwrap_or_default();
    let threshold = std::env::var("PERF_LARGE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| match std::env::var("PERF_ARCH").as_deref() {
            Ok("A" | "B") => 64 << 20,
            _ => 3 << 30,
        });
    let binary = if requested == "portable" {
        "/usr/local/bin/portable"
    } else if requested == "cascade" && option_env!("SCORER_CASCADE") == Some("1") && cascade_cpu()
    {
        "/usr/local/bin/cascade"
    } else if requested != "v3"
        && supported
        && wide_vectors()
        && (requested == "wide"
            || large_csv_input(&std::path::Path::new(&data).join("events"), threshold))
    {
        "/usr/local/bin/large"
    } else if supported {
        "/usr/local/bin/optimized"
    } else {
        "/usr/local/bin/portable"
    };
    if std::env::var_os("PROFILE").is_some_and(|p| p == "1") {
        eprintln!("selected scorer {binary}, request={requested:?}, threshold={threshold}");
    }
    let error = Command::new(binary)
        .args(std::env::args_os().skip(1))
        .exec();
    eprintln!("could not launch {binary}: {error}");
    std::process::exit(1);
}
