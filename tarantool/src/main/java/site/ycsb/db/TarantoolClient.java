/**
 * Copyright (c) 2014 - 2016 YCSB Contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import site.ycsb.*;
import site.ycsb.DBException;
import io.tarantool.*;
import io.tarantool.driver.api.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.api.connection.TarantoolConnectionSelectionStrategyType;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * YCSB binding for <a href="http://tarantool.org/">Tarantool</a>.
 */
public class TarantoolClient extends DB {
  private static final Logger LOGGER = Logger.getLogger(TarantoolClient.class.getName());

  private static final String HOSTS_PROPERTY = "tarantool.hosts";
  private static final String SPACE_PROPERTY = "tarantool.space";
  private static final String SPACE_NAME_PROPERTY = "tarantool.spaceName";
  private static final String DEFAULT_HOST = "localhost:3301";
  private static final String DEFAULT_SPACE = "1024";
  private static final String KEY_FIELD_NAME = "ycsb_id";

  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private io.tarantool.driver.api.TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> connection;

  private int spaceNo;
  private String spaceName;
  private TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> testSpace;

  private static final DefaultMessagePackMapperFactory MAPPERFACTORY = DefaultMessagePackMapperFactory.getInstance();
  private static final TarantoolTupleFactory TUPLEFACTORY =
          new DefaultTarantoolTupleFactory(MAPPERFACTORY.defaultComplexTypesMapper());

  @Override
  public void init() throws DBException {
    int idx = INIT_COUNT.incrementAndGet();
    synchronized (INIT_COUNT) {

      Properties props = getProperties();
      List<TarantoolServerAddress> addrList = new ArrayList<>();
      String hosts = props.getProperty(HOSTS_PROPERTY, DEFAULT_HOST);
      String[] hostsArr = hosts.split(",");

      for (String instance: hostsArr) {
        String[] hostPort = instance.split(":");
        addrList.add(new TarantoolServerAddress(hostPort[0], Integer.valueOf(hostPort[1])));
      }

      idx = idx % addrList.size();

      TarantoolServerAddress addr = addrList.get(idx);

      spaceName = props.getProperty(SPACE_NAME_PROPERTY);
      if (spaceName == null) {
        spaceNo = Integer.parseInt(props.getProperty(SPACE_PROPERTY, DEFAULT_SPACE));
      }

      this.connection = TarantoolClientFactory.createClient()
            .withAddress(addr.getHost(), addr.getPort())
            .withConnections(100)
            .withConnectTimeout(100)
            .withReadTimeout(100)
            .withProxyMethodMapping()
            .withCredentials("guest", "")
            .withConnectionSelectionStrategy(TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN)
            .build();

      try {
        if (spaceName != null) {
          testSpace = this.connection.space(spaceName);
        } else {
          testSpace = this.connection.space(spaceNo);
        }
      } catch (TarantoolClientException exc) {
        throw new DBException("Can't initialize Tarantool connection", exc);
      } catch (Exception exc) {
        throw new DBException("Can't initialize Tarantool connection", exc);
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.connection.close();
    } catch(Exception ex) {
      throw new DBException();
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return replace(key, values, "Can't insert element");
  }

  private HashMap<String, ByteIterator> tupleConvertFilter(TarantoolTuple input, Set<String> fields) {

    HashMap<String, ByteIterator> result = new HashMap<>();
    if (input == null) {
      return result;
    }
    for (int i = 1; i < input.size(); i=+2) {
      if (fields == null || fields.contains(input.getString(i))) {

        result.put(input.getString(i), new StringByteIterator(input.getString(i + 1)));
      }
    }
    return result;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      TarantoolResult<TarantoolTuple> selectResult = testSpace.select(Conditions.equals(KEY_FIELD_NAME, key)).join();

      HashMap<String, ByteIterator> temp = tupleConvertFilter(selectResult.get(0), fields);
      if (!temp.isEmpty()) {
        result.putAll(temp);
      }

      return Status.OK;
    } catch (TarantoolClientException ex) {
      LOGGER.log(Level.SEVERE, "Can't select element", ex);
      return Status.ERROR;
    } catch (Exception exc) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey,
                     int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    TarantoolResult<TarantoolTuple> selectResult;

    try {
      Conditions conditions = Conditions.greaterOrEquals(1, startkey).withLimit(recordcount).withOffset(0);
      selectResult = testSpace.select(conditions).join();

    } catch (TarantoolClientException exc) {
      LOGGER.log(Level.SEVERE, "Can't select range elements", exc);
      return Status.ERROR;
    } catch (Exception exc) {
      return Status.ERROR;
    }

    for (TarantoolTuple p: selectResult) {
      HashMap<String, ByteIterator> temp = tupleConvertFilter(p, fields);
      if (!temp.isEmpty()) {
        result.add((HashMap<String, ByteIterator>) temp.clone());
      }
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {

    try {
      this.testSpace.delete(Conditions.equals(KEY_FIELD_NAME, key)).join();
    } catch (TarantoolClientException exc) {
      LOGGER.log(Level.SEVERE, "Can't delete element", exc);
      return Status.ERROR;
    } catch (NullPointerException e) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return replace(key, values, "Can't replace element");
  }

  private Status replace(String key, Map<String, ByteIterator> values, String exceptionDescription) {

    List<Object> tupleList = new ArrayList<>();
    tupleList.add(null); // sharding key
    tupleList.add(key);
    for (Map.Entry<String, ByteIterator> i : values.entrySet()) {
      tupleList.add(i.getKey());
      tupleList.add(i.getValue().toString());
    }

    try {
      TarantoolTuple newTuple = TUPLEFACTORY.create(tupleList);
      this.testSpace.replace(newTuple).get();
    } catch (TarantoolClientException exc) {
      LOGGER.log(Level.SEVERE, exceptionDescription, exc);
      return Status.ERROR;
    } catch (Exception exc) {
      LOGGER.log(Level.SEVERE, exceptionDescription, exc);
      return Status.ERROR;
    }

    return Status.OK;

  }
}
