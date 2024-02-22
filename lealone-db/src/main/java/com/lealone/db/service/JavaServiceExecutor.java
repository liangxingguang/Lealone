/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.CamelCaseHelper;
import com.lealone.db.table.Column;
import com.lealone.db.value.DataType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueString;

public class JavaServiceExecutor extends ServiceExecutorBase {

    private final Service service;
    private Map<String, Method> objectMethodMap;
    private Object implementClassObject;

    public JavaServiceExecutor(Service service) {
        this.service = service;
    }

    // 第一次调用时再初始化，否则会影响启动时间
    private void init() {
        if (implementClassObject != null)
            return;
        Class<?> implementClass;
        try {
            implementClass = Class.forName(service.getImplementBy());
            implementClassObject = implementClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("newInstance exception: " + service.getImplementBy(), e);
        }
        int size = service.getServiceMethods().size();
        serviceMethodMap = new HashMap<>(size);
        objectMethodMap = new HashMap<>(size);
        if (size <= 0) {
            Method[] methods = implementClass.getDeclaredMethods();
            for (int i = 0, len = methods.length; i < len; i++) {
                Method m = methods[i];
                int modifiers = m.getModifiers();
                if (!Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
                    String name = m.getName().toUpperCase();
                    objectMethodMap.put(name, m);
                    ServiceMethod sm = new ServiceMethod();
                    sm.setMethodName(name);
                    Parameter[] parameters = m.getParameters();
                    ArrayList<Column> columns = new ArrayList<>(parameters.length);
                    for (int c = 0; c < parameters.length; c++) {
                        Parameter p = parameters[c];
                        int type = DataType.getTypeFromClass(p.getType());
                        Column column = new Column(p.getName().toUpperCase(), type);
                        columns.add(column);
                    }
                    sm.setParameters(columns);
                    sm.setReturnType(new Column("R", DataType.getTypeFromClass(m.getReturnType())));
                    serviceMethodMap.put(name, sm);
                }
            }
        } else {
            for (ServiceMethod serviceMethod : service.getServiceMethods()) {
                String serviceMethodName = serviceMethod.getMethodName();
                serviceMethodMap.put(serviceMethodName, serviceMethod);

                String objectMethodName = CamelCaseHelper.toCamelFromUnderscore(serviceMethodName);
                try {
                    // 不使用getDeclaredMethod，因为这里不考虑参数，只要方法名匹配即可
                    for (Method m : implementClass.getDeclaredMethods()) {
                        if (m.getName().equals(objectMethodName)) {
                            objectMethodMap.put(serviceMethodName, m);
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Method not found: " + objectMethodName, e);
                }
            }
        }
    }

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        init();
        Method method = objectMethodMap.get(methodName);
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return ValueNull.INSTANCE;
            return ValueString.get(ret.toString());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, Object> methodArgs) {
        init();
        Method method = objectMethodMap.get(methodName);
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return null;
            return ret.toString();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, String json) {
        init();
        Method method = objectMethodMap.get(methodName);
        Object[] args = getServiceMethodArgs(methodName, json);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return null;
            return ret.toString();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
