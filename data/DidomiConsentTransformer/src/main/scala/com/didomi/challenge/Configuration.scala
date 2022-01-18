package com.didomi.challenge

import java.util


import scala.io.Source


object Configuration {
    var configProperties: Map[String, String] = _

    /** *
     * return the value of the passing config key
     *
     * @return value of the passing key from the config
     */
    def getConfigValue(key: String): String = {

        val value = this.configProperties.get(key)

        if(value.get != null)
            value.get
        else
            null
    }

    /** *
     * reads the configuration from the specified path and store in map object
     *
     * @param configPath : path of the config file
     */
    def setConfig(configPath: String): Unit = {

        val list = Source.fromFile(configPath).getLines().toList
        configProperties = list.map { data =>
            val splittedList =  data.split("=").toList
            val listOfTupple =(splittedList.head.toString,splittedList.tail.mkString)
            listOfTupple
        }.toMap
    }
}
