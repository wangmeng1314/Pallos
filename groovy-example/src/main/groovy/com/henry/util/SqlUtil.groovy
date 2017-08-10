package com.henry.util

import groovy.sql.Sql

/**
 * 简单的SQL工具类
 */

class SqlUtil {
    final static mysqlDriver = "com.mysql.jdbc.Driver"
    final static sqlServerDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    static ThreadLocal<Sql> SQLTHREADLOCAL = new ThreadLocal<>()

    def static connectionMap = [
            url     : '',
            username: '',
            password: '',
            driver  : '',
    ]
    

    static prePareTargetConnection(String url, String userName, String passWord) {
        connectionMap.url = url
        connectionMap.username = userName
        connectionMap.password = passWord
        connectionMap.driver = mysqlDriver
    }

    static getTargetConnection() {
        if (SQLTHREADLOCAL.get() != null) {
            SQLTHREADLOCAL.set(Sql.newInstance(connectionMap))
        }
        return SQLTHREADLOCAL.get() as Sql
    }

    static executeQuery(String sql) {
        List<Object> resList = []
        getTargetConnection().eachRow(sql) {
            Object res = it.toRowReult()
            resList.add(res)
        }
        return resList
    }
}
