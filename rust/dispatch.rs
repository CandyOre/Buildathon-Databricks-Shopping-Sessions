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
        && std::is_x86_feature_detected!("avx512f")
        && std::is_x86_feature_detected!("avx512bw")
        && std::is_x86_feature_detected!("avx512cd")
        && std::is_x86_feature_detected!("avx512dq")
        && std::is_x86_feature_detected!("avx512vl")
}
#[cfg(not(target_arch = "x86_64"))]
fn optimized_cpu() -> bool {
    false
}
fn main() {
    let supported = optimized_cpu();
    let binary = if supported {
        "/usr/local/bin/optimized"
    } else {
        "/usr/local/bin/portable"
    };
    let error = Command::new(binary)
        .args(std::env::args_os().skip(1))
        .exec();
    eprintln!("could not launch {binary}: {error}");
    std::process::exit(1);
}
