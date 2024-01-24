package com.bin.spark.common;


public class BusinessException extends RuntimeException {

    private Integer code;

    public BusinessException(ResponseEnum resultEnum) {
        super(resultEnum.getDesc());

        this.code = resultEnum.getCode();
    }

    public BusinessException(Integer code, String message) {
        super(message);
        this.code = code;
    }
}
