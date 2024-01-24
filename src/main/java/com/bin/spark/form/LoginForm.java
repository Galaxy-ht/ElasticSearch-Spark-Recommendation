package com.bin.spark.form;

import lombok.Data;

import javax.validation.constraints.NotBlank;


@Data
public class LoginForm {

    @NotBlank(message = "手机号不能为空")
    private String telephone;

    @NotBlank(message = "密码不能为空")
    private String password;
}
