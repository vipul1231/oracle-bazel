package com.example.ibf.schema;

import com.example.ibf.ByteBufSerializer;
import io.netty.buffer.ByteBuf;

public class IbfTableInfoSerializer extends ByteBufSerializer<IbfTableInfo> {

//    private final ByteBufSerializer<Map<String, IbfColumnInfo>> mapByteBufSerializer =
//            nullable(mapSerializer(int16, utf8Sized, new ColumnInfoSerializer()));

    @Override
    public IbfTableInfo decode( ByteBuf byteBuf) {
//        TableRef tableRef = new TableRef(utf8Sized.decode(byteBuf), utf8Sized.decode(byteBuf));
//        return new IbfTableInfo(tableRef, mapByteBufSerializer.decode(byteBuf));
        return null;
    }

    @Override
    public void encode(IbfTableInfo value,  ByteBuf byteBuf) {
//        utf8Sized.encode(value.tableRef.schema, byteBuf);
//        utf8Sized.encode(value.tableRef.name, byteBuf);
//        mapByteBufSerializer.encode(value.columns, byteBuf);
    }

//    @Override
    public String getName() {
        return "IbfTableInfo";
    }

    private static class ColumnInfoSerializer extends ByteBufSerializer<IbfColumnInfo> {

        @Override
        public IbfColumnInfo decode( ByteBuf byteBuf) {
//            return new IbfColumnInfo(utf8Sized.decode(byteBuf), DataType.valueOf(utf8Sized.decode(byteBuf)));
            return null;
        }

        @Override
        public void encode(IbfColumnInfo value,  ByteBuf byteBuf) {
//            utf8Sized.encode(value.columnName, byteBuf);
//            utf8Sized.encode(value.destColumnType.name(), byteBuf);
        }

//        @Override
        public String getName() {
            return "IbfColumnInfo";
        }
    }
}
