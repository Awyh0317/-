package com.gec.covid19.crawler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gec.covid19.bean.CovidBean;
import com.gec.covid19.util.HttpUtils;
import com.gec.covid19.util.TimeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component

public class Covid19DataCrawler {

    @Autowired
    private KafkaTemplate kafkaTemplate;


    //@Scheduled(initialDelay = 1000,fixedDelay = 1000*60*60)
    @Scheduled(cron = "0/5 * * * * ?")

    public void testCrawling() throws Exception {
        System.out.println("每隔5s执行一次");
        String datetime = TimeUtils.format(System.currentTimeMillis(), "yyyy-MM-dd");
        //1.爬取指定页面
        String html = HttpUtils.getHtml("https://ncov.dxy.cn/ncovh5/view/pneumonia");
        //2.解析页面中的指定内容-即id为getAreaStat的标签中的全国疫情数据
        Document doc = Jsoup.parse(html);
        String text = doc.select("script[id=getAreaStat]").toString();
        //System.out.println(text);

        //3.使用正则表达式获取json格式的疫情数据
        String pattern = "\\[(.*)\\]";//定义正则规则
        Pattern reg = Pattern.compile(pattern);//编译成正则对象
        Matcher matcher = reg.matcher(text);//去text中进行匹配
        String jsonStr = "";
        if (matcher.find()) {//如果text中的内容和正则规则匹配上就取出来赋值给jsonStr变量
            jsonStr = matcher.group(0);
            System.out.println(jsonStr);
        } else {
            System.out.println("no match");
        }

        //对json数据进行更近一步的解析
        //4.将第一层json(省份数据)解析为JavaBean
        List<CovidBean> pCovidBeans = JSON.parseArray(jsonStr, CovidBean.class);


        for (CovidBean pBean : pCovidBeans) {//pBean为省份
            //System.out.println(pBean);
            //先设置时间字段
            pBean.setDatetime(datetime);
            //获取cities
            String citysStr = pBean.getCities();
            //5.将第二层json(城市数据)解析为JavaBean
            List<CovidBean> covidBeans = JSON.parseArray(citysStr, CovidBean.class);
            for (CovidBean bean : covidBeans) {//bean为城市
                //System.out.println(bean);
                bean.setDatetime(datetime);
                bean.setPid(pBean.getLocationId());//把省份的id作为城市的pid
                bean.setProvinceShortName(pBean.getProvinceShortName());
                String beanStr = JSON.toJSONString(bean);
                kafkaTemplate.send("covid19", bean.getPid(), beanStr);
            }

            String statisticsDataUrl = pBean.getStatisticsData();
            String statisticsDataStr = HttpUtils.getHtml(statisticsDataUrl);
            //获取statisticsDataStr中的data字段对应的数据
            JSONObject jsonObject = JSON.parseObject(statisticsDataStr);
            String dataStr = jsonObject.getString("data");

            //System.out.println(dataStr);
            //将爬取解析出来的每一天的数据设置回省份pBean中的StatisticsData字段中(之前该字段只是一个URL)
            pBean.setStatisticsData(dataStr);
            pBean.setCities(null);
            //System.out.println(pBean);
            //后续需要将省份疫情数据发送给Kafka
            String pBeanStr = JSON.toJSONString(pBean);
            kafkaTemplate.send("covid19",pBean.getLocationId(),pBeanStr);
        }
    }
}
