package com.example.ibf;

import io.netty.buffer.ByteBuf;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
//import org.jetbrains.annotations.NotNull;
//import org.jetbrains.annotations.Nullable;

public abstract class ByteBufSerializer<T>
        /*implements ByteBufEncoder<T>, ByteBufDecoder<T>*/ {

    public static final ByteBufSerializer<Boolean> bool;
    public static final ByteBufSerializer<Byte> byte1;
    public static final ByteBufSerializer<Short> short16;
    public static final ByteBufSerializer<Short> short16L;
    public static final ByteBufSerializer<Integer> int8;
    public static final ByteBufSerializer<Integer> int16;
    public static final ByteBufSerializer<Integer> int16L;
    public static final ByteBufSerializer<Integer> int24;
    public static final ByteBufSerializer<Integer> int24L;
    public static final ByteBufSerializer<Integer> int32;
    public static final ByteBufSerializer<Integer> int32L;
    public static final ByteBufSerializer<Long> long64;
    public static final ByteBufSerializer<Long> long64L;
    public static final ByteBufSerializer<Float> float32;
    public static final ByteBufSerializer<Float> float32L;
    public static final ByteBufSerializer<Double> double64;
    public static final ByteBufSerializer<Double> double64L;
    //    public static final StringSerializer utf8;
//    public static final SizedStringSerializer utf8Sized;
    public static final ByteBufSerializer<byte[]> byteArray;
    //    public static final SizedByteArraySerializer byteArraySized;
//    public static final ByteBufSerializer<BigInteger> bigInt;
    public static final ByteBufSerializer<BigDecimal> bigDecimal;
    public static final ByteBufSerializer<Instant> instant64;
    public static final ByteBufSerializer<Instant> instant64L;
    //    public static final ByteBufSerializer<Instant> instant96;
//    public static final ByteBufSerializer<Instant> instant96L;
//    public static final ByteBufSerializer<LocalDateTime> localDateTime64;
//    public static final ByteBufSerializer<LocalDateTime> localDateTime64L;
//    public static final ByteBufSerializer<LocalDateTime> localDateTime96;
//    public static final ByteBufSerializer<LocalDateTime> localDateTime96L;
    public static final ByteBufSerializer<LocalDate> localDate;
    public static final ByteBufSerializer<LocalDate> localDateL;

    static {
        bool =
                new ByteBufSerializer<Boolean>() {
                    public void encode(Boolean value, ByteBuf out) {
                        out.writeBoolean(value);
                    }

                    public Boolean decode(ByteBuf input) {
                        return input.readBoolean();
                    }

                    //                    @Override
                    public String getName() {
                        return "Bool";
                    }
                };
        byte1 =
                new ByteBufSerializer<Byte>() {
                    public void encode(Byte value, ByteBuf out) {
                        out.writeByte((int) value);
                    }

                    public Byte decode(ByteBuf input) {
                        return input.readByte();
                    }

                    //                    @Override
                    public String getName() {
                        return "Byte";
                    }
                };
        short16 =
                new ByteBufSerializer<Short>() {
                    public void encode(Short value, ByteBuf out) {
                        out.writeShort((int) value);
                    }

                    public Short decode(ByteBuf input) {
                        return input.readShort();
                    }

                    public String getName() {
                        return "Short16";
                    }
                };
        short16L =
                new ByteBufSerializer<Short>() {
                    public void encode(Short value, ByteBuf out) {
                        out.writeShortLE((int) value);
                    }

                    public Short decode(ByteBuf input) {
                        return input.readShortLE();
                    }

                    public String getName() {
                        return "Short16L";
                    }
                };
        int8 =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeByte(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return (int) input.readByte();
                    }

                    public String getName() {
                        return "Int8";
                    }
                };
        int16 =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeShort(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return (int) input.readShort();
                    }

                    public String getName() {
                        return "Int16";
                    }
                };
        int16L =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeShortLE(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return (int) input.readShortLE();
                    }

                    public String getName() {
                        return "Int16L";
                    }
                };
        int24 =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeMedium(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return input.readMedium();
                    }

                    public String getName() {
                        return "Int24";
                    }
                };
        int24L =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeMediumLE(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return input.readMediumLE();
                    }

                    public String getName() {
                        return "Int24L";
                    }
                };
        int32 =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeInt(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return input.readInt();
                    }

                    public String getName() {
                        return "Int32";
                    }
                };
        int32L =
                new ByteBufSerializer<Integer>() {
                    public void encode(Integer value, ByteBuf out) {
                        out.writeIntLE(value);
                    }

                    public Integer decode(ByteBuf input) {
                        return input.readIntLE();
                    }

                    public String getName() {
                        return "Int32L";
                    }
                };
        long64 =
                new ByteBufSerializer<Long>() {
                    public void encode(Long value, ByteBuf out) {
                        out.writeLong(value);
                    }

                    public Long decode(ByteBuf input) {
                        return input.readLong();
                    }

                    public String getName() {
                        return "Long64";
                    }
                };
        long64L =
                new ByteBufSerializer<Long>() {
                    public void encode(Long value, ByteBuf out) {
                        out.writeLongLE(value);
                    }

                    public Long decode(ByteBuf input) {
                        return input.readLongLE();
                    }

                    public String getName() {
                        return "Long64L";
                    }
                };
        float32 =
                new ByteBufSerializer<Float>() {
                    public void encode(Float value, ByteBuf out) {
                        out.writeFloat(value);
                    }

                    public Float decode(ByteBuf input) {
                        return input.readFloat();
                    }

                    public String getName() {
                        return "Float";
                    }
                };
        float32L =
                new ByteBufSerializer<Float>() {
                    public void encode(Float value, ByteBuf out) {
                        out.writeFloatLE(value);
                    }

                    public Float decode(ByteBuf input) {
                        return input.readFloatLE();
                    }

                    public String getName() {
                        return "FloatL";
                    }
                };
        double64 =
                new ByteBufSerializer<Double>() {
                    public void encode(Double value, ByteBuf out) {
                        out.writeDouble(value);
                    }

                    public Double decode(ByteBuf input) {
                        return input.readDouble();
                    }

                    public String getName() {
                        return "Double";
                    }
                };
        double64L =
                new ByteBufSerializer<Double>() {
                    public void encode(Double value, ByteBuf out) {
                        out.writeDoubleLE(value);
                    }

                    public Double decode(ByteBuf input) {
                        return input.readDoubleLE();
                    }

                    public String getName() {
                        return "DoubleL";
                    }
                };
//        utf8 = new StringSerializer(StandardCharsets.UTF_8);
//        utf8Sized = new SizedStringSerializer(int32, StandardCharsets.UTF_8);
        byteArray =
                new ByteBufSerializer<byte[]>() {
                    //                    @Override
                    public void encode(byte[] value, ByteBuf out) {
                        out.writeBytes(value);
                    }

                    //                    @Override
                    public byte[] decode(ByteBuf input) {
                        byte[] result = new byte[input.readableBytes()];
                        input.readBytes(result);
                        return result;
                    }

                    @Override
                    public boolean isBounded() {
                        return false;
                    }

                    //                    @Override
                    public String getName() {
                        return "ByteArray";
                    }
                };
//        byteArraySized = new SizedByteArraySerializer(int32);
//        bigInt = byteArraySized.bimap(byteArraySized, BigInteger::toByteArray, BigInteger::new);
        bigDecimal =
                new ByteBufSerializer<BigDecimal>() {
                    //                    @Override
                    public void encode(BigDecimal value, ByteBuf out) {
                        BigInteger bigInteger = value.unscaledValue();
//                        bigInt.encode(bigInteger, out);
                        out.writeInt(value.scale());
                    }

                    //                    @Override
                    public BigDecimal decode(ByteBuf input) {
//                        return new BigDecimal(bigInt.decode(input), input.readInt());
                        return null;
                    }

                    //                    @Override
                    public String getName() {
                        return "BigDecimal";
                    }
                };
        instant64 = long64.bimap(long64, Instant::toEpochMilli, Instant::ofEpochMilli);
        instant64L = long64L.bimap(long64L, Instant::toEpochMilli, Instant::ofEpochMilli);
//        instant96 = new InstantSerializer96(long64, int32);
//        instant96L = new InstantSerializer96(long64L, int32L);
//        localDateTime64 = new LocalDateTimeSerializer(instant64);
//        localDateTime64L = new LocalDateTimeSerializer(instant64L);
//        localDateTime96 = new LocalDateTimeSerializer(instant96);
//        localDateTime96L = new LocalDateTimeSerializer(instant96L);
        localDate = long64.bimap(long64, LocalDate::toEpochDay, LocalDate::ofEpochDay);
        localDateL = long64L.bimap(long64L, LocalDate::toEpochDay, LocalDate::ofEpochDay);
    }

    public static <K, V> ByteBufSerializer<Map<K, V>> mapSerializer(
            ByteBufSerializer<Integer> sizeSer,
            ByteBufSerializer<K> km,
            ByteBufSerializer<V> vm) {
//        return new MapSerializer<>(sizeSer, km, vm);
        return null;
    }

    public static <T> ByteBufSerializer<T> nullable(ByteBufSerializer<T> serializer) {
        return new ByteBufSerializer<T>() {
            public void encode(T value, ByteBuf out) {
                if (value == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
//                    serializer.encode(value, out);
                }
            }

            //
            public T decode(ByteBuf input) {
                if (input.readBoolean()) {
//                    return serializer.decode(input);
                    return null;
                }
                return null;
            }

            public boolean isBounded() {
                return serializer.isBounded();
            }

            public String getName() {
//                return "nullable(" + serializer.getName() + ')';
                return null;
            }
        };
    }

    public static <T> ByteBufSerializer<List<T>> listSerializer(
            ByteBufSerializer<Integer> sizeSer, ByteBufSerializer<T> itemSer) {
//        return new ListSerializer<>(sizeSer, itemSer);
        return null;
    }

    public static <T> ByteBufSerializer<Set<T>> setSerializer(
            ByteBufSerializer<Integer> sizeSer, ByteBufSerializer<T> itemSer) {
//        return new SetSerializer<>(sizeSer, itemSer);
        return null;
    }

    public static <T> ByteBufSerializer<T> sized(ByteBufSerializer<Integer> sizeSer, ByteBufSerializer<T> itemSer) {
//        return new SizedSerializer<>(sizeSer, itemSer);
        return null;
    }

//    public static <T> EncryptedSerializer<T> encrypted(
//            ByteBufSerializer<T> serializer, ObjectPool<Cipher> encodePool, ObjectPool<Cipher> decodePool) {
//        return new EncryptedSerializer<>(serializer, encodePool, decodePool);
//    }

    public ByteBufSerializer() {
    }

    public boolean isBounded() {
        return true;
    }

    <V> ByteBufSerializer<V> bimap(
            ByteBufSerializer<T> serializer, Function<V, T> enc, Function<T, V> dec) {
        return new ByteBufSerializer<V>() {
            //            @Override
            public void encode(V value, ByteBuf out) {
//                serializer.encode(enc.apply(value), out);
            }

            //            @Override
            public V decode(ByteBuf input) {
//                return dec.apply(serializer.decode(input));
                return null;
            }

            @Override
            public boolean isBounded() {
                return serializer.isBounded();
            }

            //            @Override
            public String getName() {
//                return serializer.getName();
                return null;
            }
        };
    }

    public T decode(ByteBuf byteBuf) {
        return null;
    }

    public void encode(T keyLengthsSum, ByteBuf byteBuf) {

    }
}
