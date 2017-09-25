package com.finaxys.bigdata.training.batch;

import java.io.Serializable;

/**
 * Class representing orders data. It will be used by Spark Dataframe to get the schema from reflection
 */
public class Order implements Serializable {
    private String type;
    private String orderbook;
    private String sender;
    private String extid;
    private String nature;
    private String dir;
    private Integer price;
    private Integer quantity;
    private Integer orderValidity;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrderbook() {
        return orderbook;
    }

    public void setOrderbook(String orderbook) {
        this.orderbook = orderbook;
    }

    public String getSender() {
        return sender;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public String getExtid() {
        return extid;
    }

    public void setExtid(String extid) {
        this.extid = extid;
    }

    public String getNature() {
        return nature;
    }

    public void setNature(String nature) {
        this.nature = nature;
    }

    public String getDir() {
        return dir;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Integer getOrderValidity() {
        return orderValidity;
    }

    public void setOrderValidity(Integer orderValidity) {
        this.orderValidity = orderValidity;
    }

    public Order toOrder(String row) {
        String[] orderCols = row.split(";");
        if (orderCols.length != 9) {
            throw new IllegalArgumentException("Couldn't parse the line from orders file");
        }
        Order order = new Order();
        order.setType(orderCols[0]);
        order.setOrderbook(orderCols[1]);
        order.setSender(orderCols[2]);
        order.setExtid(orderCols[3]);
        order.setNature(orderCols[4]);
        order.setDir(orderCols[5]);
        order.setPrice(Integer.parseInt(orderCols[6]));
        order.setQuantity(Integer.parseInt(orderCols[7]));
        order.setOrderValidity(Integer.parseInt(orderCols[8]));

        return order;
    }
}
