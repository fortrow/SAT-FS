package com.fortrow.cxnfs.transport;

import tech.kwik.core.QuicStream;

import java.lang.reflect.Field;

public final class QuicStreamSendBufferTuner {
    private QuicStreamSendBufferTuner() {
    }

    public static boolean tune(QuicStream stream, int bufferBytes) {
        if (stream == null || bufferBytes <= 0) {
            return false;
        }
        try {
            Object outputStream = field(stream.getClass(), "outputStream").get(stream);
            Field outputMax = field(outputStream.getClass(), "maxBufferSize");
            outputMax.setInt(outputStream, bufferBytes);

            Object sendBuffer = field(outputStream.getClass(), "sendBuffer").get(outputStream);
            Field sendBufferMax = field(sendBuffer.getClass(), "maxBufferSize");
            sendBufferMax.setInt(sendBuffer, bufferBytes);
            return true;
        } catch (ReflectiveOperationException | RuntimeException e) {
            return false;
        }
    }

    private static Field field(Class<?> type, String name) throws NoSuchFieldException {
        Class<?> cursor = type;
        while (cursor != null) {
            try {
                Field field = cursor.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException e) {
                cursor = cursor.getSuperclass();
            }
        }
        throw new NoSuchFieldException(type.getName() + "." + name);
    }
}
