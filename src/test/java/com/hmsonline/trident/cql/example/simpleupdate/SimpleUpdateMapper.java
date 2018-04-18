package com.hmsonline.trident.cql.example.simpleupdate;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

public class SimpleUpdateMapper implements CqlRowMapper<Object, Object>, Serializable {
    private static final long serialVersionUID = 1L;

    public Statement map(TridentTuple tuple) {
        long t = System.currentTimeMillis() % 10;
        Update statement = update("mykeyspace1", "mytable1");
        statement.with(set("col1", tuple.getString(0))).where(eq("t", t));
//        Select statement = select().all().from("mykeyspace1", "mytable1");

        return statement;
    }

    public Statement map(TridentTuple tuple, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement retrieve(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement map(Object key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getValue(Row row) {
        // TODO Auto-generated method stub
        return null;
    }
}
