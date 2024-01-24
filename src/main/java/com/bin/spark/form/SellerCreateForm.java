package com.bin.spark.form;

import lombok.Data;

import javax.validation.constraints.NotBlank;


@Data
public class SellerCreateForm {

    @NotBlank(message = "商户名不能为空")
    private String name;

}
