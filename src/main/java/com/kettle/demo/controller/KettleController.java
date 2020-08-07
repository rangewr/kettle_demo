/*
 * @Author: wangran
 * @Date: 2020-06-04 15:07:43
 * @LastEditors: wangran
 * @LastEditTime: 2020-06-18 10:29:49
 */
package com.kettle.demo.controller;

import java.io.File;
import java.util.Map;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cn.hutool.core.io.FileUtil;

@RestController
@RequestMapping("kettle")
public class KettleController {

    @Autowired
    private static Map<String, Trans> objectMap;

    public static Map<String, Trans> getObjectMap() {
        return objectMap;
    }

    public static void setObjectMap(String key, Trans value) {
        objectMap.put(key, value);
    }

    @RequestMapping("testKettle")
    public String testKettle() throws KettleException {

        // 可以把文件放到resources目录下面，然后用hutool读取这文件
        // File file = FileUtil.file("33.ktr");
        // 这是我本地进行测试的，所以我把文件放到了桌面上了，写了根目录
        File file = FileUtil.file(
                "C:/Users/Administrator/workspace/vsCode/XADPwdManage/files/ad9899a0dfcdeb07c464af8e487d52e6/tempfile.txt.ktr");
        String path = file.getPath();
        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(
                "E:\\迅雷下载\\pdi-ce-8.2.0.0-342\\data-integration\\plugins\\steps\\pentaho-kafka-producer", false, true));
        // 初始化
        KettleEnvironment.init();
        // 加载文件
        TransMeta transMeta = new TransMeta(path);
        Trans trans = new Trans(transMeta);
        // 放入参数，这里其实可以从数据库中取到
        // 如果没有参数，可以把这步忽略
        trans.setParameterValue("stade", "2019-04-24");
        trans.prepareExecution(null);
        trans.startThreads();
        // 等待执行完毕
        trans.waitUntilFinished();

        // // 初始化
        // KettleEnvironment.init();
        // // 加载文件
        // File file = FileUtil.file("C:\\Users\\Y\\Desktop\\33.ktr");
        // TransMeta transMeta = new
        // TransMeta("C:\\Users\\Administrator\\Desktop\\deskTopFolder\\springboot_kettle.ktr");
        // Trans trans = new Trans(transMeta);
        // // 放入参数，这里其实可以从数据库中取到，如果没有参数，可以把这步忽略
        // // trans.setParameterValue("stade", "2019-04-24");
        // trans.prepareExecution(null);
        // trans.startThreads();
        // trans.waitUntilFinished();
        return "success";
    }

    @RequestMapping("pauseKettle")
    public String pauseKettle() throws KettleException {

        // 可以把文件放到resources目录下面，然后用hutool读取这文件
        // File file = FileUtil.file("33.ktr");
        // 这是我本地进行测试的，所以我把文件放到了桌面上了，写了根目录
        File file = FileUtil.file(
                "C:/Users/Administrator/workspace/vsCode/XADPwdManage/files/9ac805dd99b3c1c869f9e1f8a8093bca/linkedin_cracked.txt.ktr");
        String path = file.getPath();
        StepPluginType.getInstance().getPluginFolders().add(new PluginFolder(
                "E:\\迅雷下载\\pdi-ce-8.2.0.0-342\\data-integration\\plugins\\steps\\pentaho-kafka-producer", false, true));
        // 初始化
        KettleEnvironment.init();
        // 加载文件
        TransMeta transMeta = new TransMeta(path);
        Trans trans = new Trans(transMeta);
        // 放入参数，这里其实可以从数据库中取到
        // 如果没有参数，可以把这步忽略
        trans.setParameterValue("stade", "2019-04-24");
        trans.prepareExecution(null);
        trans.startThreads();
        try {
            Thread.sleep(5 * 1000);// 暂停10秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        trans.pauseRunning();
        setObjectMap("test", trans);
        Trans newTrans = getObjectMap().get("test");
        System.out.println("before pause");

        try {
            Thread.sleep(10 * 1000);// 暂停10秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("after pause");
        newTrans.resumeRunning();
        // 等待执行完毕
        trans.waitUntilFinished();

        return "success";
    }
}