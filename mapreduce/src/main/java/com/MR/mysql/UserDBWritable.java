package com.MR.mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UserDBWritable implements DBWritable, Writable {
    private String name;
    private String address;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(address);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        address = dataInput.readUTF();
    }

    /**
     *
     * @param preparedStatement
     * @throws SQLException
     * 写入DB
     */
    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,name);
        preparedStatement.setString(2,address);
    }

    /**
     *
     * @param resultSet
     * @throws SQLException
     * 读DB
     */
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        name = resultSet.getString(1);
        address = resultSet.getString(2);
    }
}
