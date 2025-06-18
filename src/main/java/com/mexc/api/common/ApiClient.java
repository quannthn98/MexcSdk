package com.mexc.api.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import okhttp3.*;
import okhttp3.logging.HttpLoggingInterceptor;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ApiClient {
    private final String requestHost;
    private final OkHttpClient okHttpClient;
    private final String accessKey;
    private final String secretKey;

    public ApiClient(String baseUrl, String accessKey, String secretKey) {
        this.requestHost = baseUrl;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.okHttpClient = createOkHttpClient();
    }

    public OkHttpClient createOkHttpClient() {
        HttpLoggingInterceptor httpLoggingInterceptor = new HttpLoggingInterceptor();
        httpLoggingInterceptor.setLevel(HttpLoggingInterceptor.Level.BODY);
        return new OkHttpClient.Builder()
                .connectTimeout(45, TimeUnit.SECONDS)
                .readTimeout(45, TimeUnit.SECONDS)
                .writeTimeout(45, TimeUnit.SECONDS)
                .addInterceptor(httpLoggingInterceptor)
                .addInterceptor(new SignatureInterceptor(secretKey, accessKey))
                .build();
    }


    public <T> T get(String uri, Map<String, String> params, TypeReference<T> ref) {
        try {
            Response response = okHttpClient
                    .newCall(new Request.Builder().url(requestHost + uri + "?" + SignatureUtil.toQueryString(params)).get().build())
                    .execute();
            return handleResponse(response, ref);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T post(String uri, Map<String, String> params, TypeReference<T> ref) {
        try {
            Response response = okHttpClient
                    .newCall(new Request.Builder()
                            .url(requestHost.concat(uri))
                            .post(RequestBody.create(SignatureUtil.toQueryString(params), MediaType.get("text/plain"))).build()).execute();
            return handleResponse(response, ref);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T postEmptyBody(String uri, Map<String, String> params, TypeReference<T> ref) {
        try {
            String timestamp = Instant.now().toEpochMilli() + "";
            String paramsStr = SignatureUtil.toQueryStringWithEncoding(params);
            paramsStr += "&timestamp=" + timestamp;
            String signature = SignatureUtil.actualSignature(paramsStr, secretKey);
            paramsStr += "&signature=" + signature;


            RequestBody empty = RequestBody.create(null, new byte[0]);
            Request.Builder body = new Request.Builder().url(requestHost.concat(uri).concat("?").concat(paramsStr)).method("POST", empty).header("Content-Length", "0");
            Response response = okHttpClient
                    .newCall(body.build()).execute();
            return handleResponse(response, ref);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T put(String uri, Map<String, String> params, TypeReference<T> ref) {
        try {
            Response response = okHttpClient
                    .newCall(new Request.Builder()
                            .url(requestHost.concat(uri))
                            .put(RequestBody.create(SignatureUtil.toQueryString(params), MediaType.get("text/plain"))).build()).execute();
            return handleResponse(response, ref);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public <T> T delete(String uri, Map<String, String> params, TypeReference<T> ref) {
        try {
            return handleResponse(okHttpClient
                    .newCall(new Request.Builder()
                            .url(requestHost.concat(uri))
                            .delete(RequestBody.create(SignatureUtil.toQueryString(params), MediaType.get("text/plain"))).build()).execute(), ref);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private <T> T handleResponse(Response response, TypeReference<T> ref) throws IOException {
        Gson gson = new Gson();
        assert response.body() != null;
        String content = response.body().string();
        if (200 != response.code()) {
            throw new RuntimeException(content);
        }
        return gson.fromJson(content, ref.getType());
    }


}
