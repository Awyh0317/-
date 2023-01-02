package com.gec.covid19ui.controller;

import com.gec.covid19ui.bean.Result;
import com.gec.covid19ui.mapper.CovidMapper;
import org.apache.commons.lang3.time.FastDateFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/5/29 11:16
 * Desc 用来接收前端数据请求的Controller
 */
@RestController//=@Controller+@ResponseBody //表示该类是SpringBoot的一个Controller,且返回的数据为Json格式
@RequestMapping("covid")
public class Covid19Controller {

    @Autowired
    private CovidMapper covidMapper;

    /**
     * 接收前端请求返回全国疫情汇总数据
     */
    @RequestMapping("getNationalData")
    public Result getNationalData(){
        String datetime = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis());
        Map<String, Object> data = covidMapper.getNationalData(datetime).get(0);
        Result result = Result.success(data);
        return result;
    }

    //getNationalMapData
    /**
     * 查询全国各省份累计确诊数据并返回
     */
    @RequestMapping("getNationalMapData")
    public Result getNationalMapData(){
        String datetime = FastDateFormat.getInstance("yyyy-MM-dd").format(System.currentTimeMillis());
        List<Map<String, Object>> data =  covidMapper.getNationalMapData(datetime);
        return Result.success(data);
    }


    //getCovidTimeData
    /**
     * 查询全国每一天的疫情数据并返回
     */
    @RequestMapping("getCovidTimeData")
    public Result getCovidTimeData(){
        List<Map<String, Object>> data =  covidMapper.getCovidTimeData();
        return Result.success(data);
    }




    //getCovidWz
    /**
     * 查询各物资使用情况
     */
    @RequestMapping("getCovidWz")
    public Result getCovidWz(){
        List<Map<String, Object>> data = covidMapper.getCovidWz();
        return Result.success(data);
    }

}
