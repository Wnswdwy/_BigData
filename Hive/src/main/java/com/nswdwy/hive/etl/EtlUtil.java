package com.nswdwy.hive.etl;

import javax.security.sasl.Sasl;

/**
 * @author yycstart
 * @create 2020-09-22 14:15
 *
 *
 *   * 清洗规则:
 *   1. 长度小于9的不要
 *   2. 将视频类别的中的空格去掉  People & Blogs  ==>People&Blogs
 *   3. 将关联视频通过&拼接  e2k0h6tPvGc	yuO6yjlvXe8  ==>e2k0h6tPvGc&yuO6yjlvXe8
 *
 */
public class EtlUtil {
    public static String eDail(String line){
        String[] split = line.split("\t");
        if(split.length < 9){
            return null;
        }
        String s = split[3].replaceAll(" ", "");
        StringBuffer str = new StringBuffer();
        for (int i = 0; i < split.length; i++) {
            if(i < 9) {
                if (i == split.length) {
                    str.append(split[i]);
                } else {
                    str.append(split[i]).append("\t");
                }
            }else{
                if(i == split.length - 1){
                    str.append(split[i]);
                }else{
                    str.append("&").append(split[i]);
                }
            }
        }
        return str.toString();

    }
}
