package com.fortrow.cxnfs.protocol;

public enum ControlOperation {
    AUTH_START(1),
    AUTH_ACCEPTED(2),
    AUTH_CHALLENGE(3),
    AUTH_PROOF(4),
    LIST_DIRECTORY(10),
    LIST_DIRECTORY_RESULT(12),
    STAT_PATH(11),
    STAT_PATH_RESULT(13),
    PUT_FILE(20),
    PUT_FILE_RESULT(21),
    GET_MANIFEST(30),
    MANIFEST_RESULT(31),
    GET_RECONSTRUCTION_PLAN(40),
    RECONSTRUCTION_PLAN_RESULT(41),
    READ_FILE_RANGE(42),
    READ_FILE_RANGE_RESULT(43),
    READ_CONTAINER_RANGE(50),
    RENAME_PATH(60),
    DELETE_PATH(61),
    RENAME_PATH_RESULT(62),
    DELETE_PATH_RESULT(63),
    MOUNT_NEGOTIATE(70),
    MOUNT_NEGOTIATED(71),
    TRANSFER_BOOTSTRAP(80),
    TRANSFER_ACCEPTED(81),
    TRANSFER_METADATA(82),
    TRANSFER_COMMIT(83),
    TRANSFER_COMMITTED(84),
    TRANSFER_ENTRY_UPDATE(85),
    TRANSFER_ENTRY_COMMIT(86),
    TRANSFER_ENTRY_COMMITTED(87),
    TRANSFER_METADATA_ACCEPTED(88),
    TRANSFER_ENTRY_ACCEPTED(89),
    ERROR(100);

    private final int code;

    ControlOperation(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static ControlOperation fromCode(int code) {
        for (ControlOperation operation : values()) {
            if (operation.code == code) {
                return operation;
            }
        }
        throw new IllegalArgumentException("unknown control operation code: " + code);
    }
}
