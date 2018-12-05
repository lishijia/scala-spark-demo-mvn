package lishijia.streaming.scenic.kafka;

import java.io.Serializable;

public class Order implements Serializable{

    private String scenic_code;
    private String scenic_name;

    private String channel_name;
    private String channel_code;

    private String order_no;
    private String place_time;

    private String settle_price;
    private String settle_amount;

    private String certificate_no;
    private String mobile_no;

    private String buy_number;

    public String getScenic_code() {
        return scenic_code;
    }

    public void setScenic_code(String scenic_code) {
        this.scenic_code = scenic_code;
    }

    public String getScenic_name() {
        return scenic_name;
    }

    public void setScenic_name(String scenic_name) {
        this.scenic_name = scenic_name;
    }

    public String getChannel_name() {
        return channel_name;
    }

    public void setChannel_name(String channel_name) {
        this.channel_name = channel_name;
    }

    public String getChannel_code() {
        return channel_code;
    }

    public void setChannel_code(String channel_code) {
        this.channel_code = channel_code;
    }

    public String getOrder_no() {
        return order_no;
    }

    public void setOrder_no(String order_no) {
        this.order_no = order_no;
    }

    public String getPlace_time() {
        return place_time;
    }

    public void setPlace_time(String place_time) {
        this.place_time = place_time;
    }

    public String getSettle_price() {
        return settle_price;
    }

    public void setSettle_price(String settle_price) {
        this.settle_price = settle_price;
    }

    public String getSettle_amount() {
        return settle_amount;
    }

    public void setSettle_amount(String settle_amount) {
        this.settle_amount = settle_amount;
    }

    public String getCertificate_no() {
        return certificate_no;
    }

    public void setCertificate_no(String certificate_no) {
        this.certificate_no = certificate_no;
    }

    public String getMobile_no() {
        return mobile_no;
    }

    public void setMobile_no(String mobile_no) {
        this.mobile_no = mobile_no;
    }

    public String getBuy_number() {
        return buy_number;
    }

    public void setBuy_number(String buy_number) {
        this.buy_number = buy_number;
    }
}
