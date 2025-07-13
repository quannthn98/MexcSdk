package com.mexc.api.common.constant;

public interface StreamChannel {
    String PRIVATE_LOGIN = "rs.login";
    String PRIVATE_ORDER = "push.personal.order";
    String PRIVATE_ASSET = "push.personal.asset";
    String PRIVATE_POSITION = "push.personal.position";
    String PUBLIC_TICKERS = "push.tickers";
    String PUBLIC_DEPTH = "push.depth";
    String PUBLIC_FUNDING_RATE = "push.funding.rate";
}
