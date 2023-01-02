package com.gec.covid19.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Date 2020/5/27 21:31
 * Desc 用来封装防疫物资的JavaBean
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MateriaBean {
    private String name;//物资名称
    private String from;//物质来源
    private Integer count;//物资数量
}
