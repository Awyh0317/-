package com.gec.covid19.generator;



import com.alibaba.fastjson.JSON;
import com.gec.covid19.bean.MateriaBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;
/**
 * Date 2020/5/27 21:28
 * Desc 使用程序模拟生成疫情数据
 */

public class test {

    public void generator() {
        System.out.println("生成物资数据......");
        Random ran = new Random();
        for (int i = 0; i < 10; i++) {
            MateriaBean materiaBean = new MateriaBean(wzmc[ran.nextInt(wzmc.length)], wzly[ran.nextInt(wzly.length)], ran.nextInt(1000));
            System.out.println(materiaBean);

        }
    }

    private String[] wzmc = new String[]
            {
                    "N95口罩/个",
                    "医用外科口罩/个",
                    "84消毒液/瓶",
                    "电子体温计/个",
                    "一次性手套/副",
                    "护目镜/副",
                    "医用防护服/套"};

    //物质来源
    private String[] wzly = new String[]{"采购", "下拨", "捐赠", "消耗", "需求"};

}
