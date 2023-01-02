package com.gec.covid19ui.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Author itcast
 * Date 2020/5/29 11:36
 * Desc
 */
@Mapper
@Component
public interface CovidMapper {

    /**
     * 查询全国疫情汇总数据
     * @param datetime
     * @return
     */
    @Select("select `datetime`, `currentConfirmedCount`, `confirmedCount`, `suspectedCount`, `curedCount`, `deadCount` from covid19_1 where datetime = #{datetime}")
    List<Map<String,Object>> getNationalData(String datetime);


    /**
     * 查询全国各省份疫情累计确诊数据
     * @param datetime
     * @return 省份名称,累计确诊数
     */
    @Select("select provinceShortName as name ,confirmedCount as value from covid19_2 where datetime = #{datetime}")
    List<Map<String, Object>> getNationalMapData(String datetime);

    /**
     * 查询全国每一天的疫情数据
      * @return
     */
    //'新增确诊', '累计确诊', '疑似病例', '累计治愈', '累计死亡'
    @Select("select dateId,confirmedIncr as `新增确诊`,confirmedCount as `累计确诊`,suspectedCount as `疑似病例`,curedCount as `累计治愈`,deadCount as `累计死亡` from covid19_3")
    List<Map<String, Object>> getCovidTimeData();


    /**
     * 查询防疫物资使用情况
     * @return
     */
    //INSERT INTO `bigdata`.`covid19_wz` (`name`, `cg`, `xb`, `jz`, `xh`, `xq`, `kc`)
    //'name', '采购', '下拨', '捐赠', '消耗', '需求', '库存'
    @Select("select name , `cg` as `采购`, `xb` as `下拨`, `jz` as `捐赠`, `xh` as `消耗`, `xq` as `需求`, `kc` as `库存` from covid19_wz")
    List<Map<String, Object>> getCovidWz();

}
