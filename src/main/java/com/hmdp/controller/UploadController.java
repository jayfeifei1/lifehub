package com.hmdp.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("upload")
public class UploadController {

    /**
     * 上传根目录，来自 application.yaml 的 hmdp.upload-dir（支持 UPLOAD_DIR 环境变量覆盖，容器部署无需改代码）
     */
    @Value("${hmdp.upload-dir}")
    private String uploadDir;

    @PostMapping("blog")
    public Result uploadImage(@RequestParam("file") MultipartFile image) {
        try {
            // 获取原始文件名称
            String originalFilename = image.getOriginalFilename();
            // 生成新文件名（相对路径，兼容 Windows/Linux）
            String fileName = createNewFileName(originalFilename);
            // 保存文件
            image.transferTo(new File(uploadDir, fileName));
            // 返回结果（带 / 前缀，前端拼 /imgs + 返回值）
            log.debug("文件上传成功，{}", fileName);
            return Result.ok("/" + fileName);
        } catch (IOException e) {
            throw new RuntimeException("文件上传失败", e);
        }
    }

    /**
     * 删除博客图片。
     * POST + 登录态校验（/upload/** 已从拦截器放行名单移除）+ 目录穿越防护。
     */
    @PostMapping("/blog/delete")
    public Result deleteBlogImg(@RequestParam("name") String filename) {
        if (StrUtil.isBlank(filename)) {
            return Result.fail("文件名称不能为空");
        }
        //1.兼容前端可能携带的 /imgs 前缀，并转为相对路径
        String relative = filename;
        if (relative.startsWith("/imgs/")) {
            relative = relative.substring("/imgs/".length());
        }
        relative = relative.startsWith("/") ? relative.substring(1) : relative;

        //2.规范化并校验目标文件必须位于上传目录内（防目录穿越，如 ../、绝对路径）
        Path uploadRoot = Paths.get(uploadDir).toAbsolutePath().normalize();
        Path target = uploadRoot.resolve(relative).normalize();
        if (!target.startsWith(uploadRoot)) {
            log.warn("拒绝删除越界文件: {}", filename);
            return Result.fail("非法文件名称");
        }

        //3.删除
        File file = target.toFile();
        if (file.isDirectory() || !file.exists()) {
            return Result.fail("文件不存在");
        }
        FileUtil.del(file);
        return Result.ok();
    }

    /**
     * 生成按 UUID 哈希散列的文件路径。
     * 返回相对路径（无前导斜杠）：new File(parent, child) 在 child 以 "/" 开头时会忽略 parent，
     * 之前带前导 "/" 的写法在 Windows/Linux 上都会把文件写到错误位置。
     */
    private String createNewFileName(String originalFilename) {
        // 获取后缀
        String suffix = StrUtil.subAfter(originalFilename, ".", true);
        // 生成目录
        String name = UUID.randomUUID().toString();
        int hash = name.hashCode();
        int d1 = hash & 0xF;
        int d2 = (hash >> 4) & 0xF;
        // 判断目录是否存在
        File dir = new File(uploadDir, StrUtil.format("blogs/{}/{}", d1, d2));
        if (!dir.exists()) {
            dir.mkdirs();
        }
        // 生成文件名
        return StrUtil.format("blogs/{}/{}/{}.{}", d1, d2, name, suffix);
    }
}
