package com.fortrow.cxnfs.storage;

public record LinuxBlockfsControlResult(
        boolean accepted,
        boolean unsupported,
        String output
) {
    public LinuxBlockfsControlResult {
        output = output == null ? "" : output;
        if (accepted == unsupported) {
            throw new IllegalArgumentException("result must be accepted or unsupported");
        }
    }

    public static LinuxBlockfsControlResult accepted(String output) {
        return new LinuxBlockfsControlResult(true, false, output);
    }

    public static LinuxBlockfsControlResult unsupported(String output) {
        return new LinuxBlockfsControlResult(false, true, output);
    }
}
