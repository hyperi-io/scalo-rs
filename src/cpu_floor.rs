// Project:   scalo
// File:      src/cpu_floor.rs
// Purpose:   Refuse to start on an x86_64 CPU below the build's floor
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A startup check for binaries built with `-C target-cpu=x86-64-v3` and
//! `+pclmulqdq`.
//!
//! Such a binary dies with SIGILL on an older CPU, or on a VM whose CPU type
//! masks AVX2, and says nothing about why. [`require_cpu_floor!`](crate::require_cpu_floor) adds a check
//! that runs from `.init_array` before `main` and the Rust runtime. It names
//! the missing feature group and the floor on stderr, then exits with status 1.
//!
//! ```ignore
//! scalo::require_cpu_floor!();
//!
//! fn main() {
//!     // ...
//! }
//! ```
//!
//! Call it once, at item level in the binary crate's `main.rs`. It has to
//! expand there: an `.init_array` entry inside a library is linked only when
//! something else references its object file. It expands to nothing unless the
//! build targets x86_64 Linux with AVX2 enabled, so a baseline build carries no
//! check.
//!
//! The check is assembly, not Rust, because a v3 build lets the compiler use
//! BMI2, MOVBE or AVX in any Rust code, the check included. It reads CPUID and
//! XCR0 and makes raw `write` and `exit_group` syscalls, so nothing it runs
//! needs more than the baseline instruction set.
//!
//! What it checks, in order:
//!
//! | Check | Feature group |
//! |---|---|
//! | CPUID leaf 0 reports leaf 7 | the CPUID range a VM may cap |
//! | leaf 1 ECX | SSE3, PCLMULQDQ, SSSE3, FMA, CMPXCHG16B, SSE4.1, SSE4.2, MOVBE, POPCNT, OSXSAVE, AVX, F16C |
//! | XCR0 bits 1 and 2 | the OS saves SSE and AVX state |
//! | leaf 7 EBX | BMI1, AVX2, BMI2 |
//! | leaf 0x80000000 reports 0x80000001 | the extended CPUID range |
//! | leaf 0x80000001 ECX | LAHF/SAHF, LZCNT |

/// Refuse to start on an x86_64 CPU below x86-64-v3 with PCLMULQDQ.
///
/// See the [module docs](crate::cpu_floor) for where to call it and what it
/// checks.
#[macro_export]
macro_rules! require_cpu_floor {
    () => {
        #[cfg(all(
            target_arch = "x86_64",
            target_os = "linux",
            target_feature = "avx2"
        ))]
        ::core::arch::global_asm!(
            ".pushsection .text.cpu_floor_check,\"ax\",@progbits",
            ".p2align 4",
            ".type cpu_floor_check,@function",
            "cpu_floor_check:",
            "    pushq %rbx",
            "    xorl %eax, %eax",
            "    xorl %ecx, %ecx",
            "    cpuid",
            "    cmpl $7, %eax",
            "    jb .Lcpu_floor_range",
            "    movl $1, %eax",
            "    xorl %ecx, %ecx",
            "    cpuid",
            "    andl $0x38D83203, %ecx",
            "    cmpl $0x38D83203, %ecx",
            "    jne .Lcpu_floor_leaf1",
            "    xorl %ecx, %ecx",
            "    xgetbv",
            "    andl $6, %eax",
            "    cmpl $6, %eax",
            "    jne .Lcpu_floor_xcr0",
            "    movl $7, %eax",
            "    xorl %ecx, %ecx",
            "    cpuid",
            "    andl $0x128, %ebx",
            "    cmpl $0x128, %ebx",
            "    jne .Lcpu_floor_leaf7",
            "    movl $0x80000000, %eax",
            "    xorl %ecx, %ecx",
            "    cpuid",
            "    cmpl $0x80000001, %eax",
            "    jb .Lcpu_floor_ext",
            "    movl $0x80000001, %eax",
            "    xorl %ecx, %ecx",
            "    cpuid",
            "    andl $0x21, %ecx",
            "    cmpl $0x21, %ecx",
            "    jne .Lcpu_floor_ext",
            "    popq %rbx",
            "    ret",
            ".Lcpu_floor_range:",
            "    leaq .Lcpu_floor_msg_range(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_range_end - .Lcpu_floor_msg_range), %edx",
            "    jmp .Lcpu_floor_refuse",
            ".Lcpu_floor_leaf1:",
            "    leaq .Lcpu_floor_msg_leaf1(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_leaf1_end - .Lcpu_floor_msg_leaf1), %edx",
            "    jmp .Lcpu_floor_refuse",
            ".Lcpu_floor_xcr0:",
            "    leaq .Lcpu_floor_msg_xcr0(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_xcr0_end - .Lcpu_floor_msg_xcr0), %edx",
            "    jmp .Lcpu_floor_refuse",
            ".Lcpu_floor_leaf7:",
            "    leaq .Lcpu_floor_msg_leaf7(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_leaf7_end - .Lcpu_floor_msg_leaf7), %edx",
            "    jmp .Lcpu_floor_refuse",
            ".Lcpu_floor_ext:",
            "    leaq .Lcpu_floor_msg_ext(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_ext_end - .Lcpu_floor_msg_ext), %edx",
            ".Lcpu_floor_refuse:",
            "    movl $2, %edi",
            "    movl $1, %eax",
            "    syscall",
            "    leaq .Lcpu_floor_msg_floor(%rip), %rsi",
            "    movl $(.Lcpu_floor_msg_floor_end - .Lcpu_floor_msg_floor), %edx",
            "    movl $2, %edi",
            "    movl $1, %eax",
            "    syscall",
            "    movl $1, %edi",
            "    movl $231, %eax",
            "    syscall",
            "    ud2",
            ".size cpu_floor_check, . - cpu_floor_check",
            ".popsection",
            ".pushsection .rodata.cpu_floor_check,\"a\",@progbits",
            ".Lcpu_floor_msg_range:",
            ".ascii \"error: this CPU reports no CPUID leaf 7, so AVX2 can't be confirmed.\\n\"",
            ".Lcpu_floor_msg_range_end:",
            ".Lcpu_floor_msg_leaf1:",
            ".ascii \"error: this CPU lacks one of SSE3, PCLMULQDQ, SSSE3, FMA, CMPXCHG16B, SSE4.1, SSE4.2, MOVBE, POPCNT, OSXSAVE, AVX or F16C.\\n\"",
            ".Lcpu_floor_msg_leaf1_end:",
            ".Lcpu_floor_msg_xcr0:",
            ".ascii \"error: the OS hasn't enabled AVX register state (XCR0).\\n\"",
            ".Lcpu_floor_msg_xcr0_end:",
            ".Lcpu_floor_msg_leaf7:",
            ".ascii \"error: this CPU lacks one of AVX2, BMI1 or BMI2.\\n\"",
            ".Lcpu_floor_msg_leaf7_end:",
            ".Lcpu_floor_msg_ext:",
            ".ascii \"error: this CPU lacks LZCNT or LAHF/SAHF, or caps the extended CPUID range.\\n\"",
            ".Lcpu_floor_msg_ext_end:",
            ".Lcpu_floor_msg_floor:",
            ".ascii \"This build needs an x86-64-v3 CPU with PCLMULQDQ: Intel Haswell (2013) or later, AMD Excavator (2015) or EPYC 7001 or later. On a VM, set the CPU type to host, x86-64-v3 or a Haswell EVC baseline.\\n\"",
            ".Lcpu_floor_msg_floor_end:",
            ".popsection",
            ".pushsection .init_array,\"aw\",@init_array",
            ".p2align 3",
            ".quad cpu_floor_check",
            ".popsection",
            options(att_syntax)
        );
    };
}

#[cfg(test)]
mod tests {
    /// Every refusal ends with this line, and release gates match on it.
    const FLOOR_LINE: &str = "This build needs an x86-64-v3 CPU with PCLMULQDQ:";

    #[test]
    fn the_floor_line_is_stable() {
        let source = include_str!("cpu_floor.rs");
        assert!(
            source.contains(&format!(".ascii \\\"{FLOOR_LINE}")),
            "the floor line changed: tell the release gates first"
        );
    }
}
