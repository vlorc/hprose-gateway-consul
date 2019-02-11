<p align="center"><img src="http://hprose.com/banner.@2x.png" alt="Hprose" title="Hprose" width="650" height="200" /></p>

# [Hprose-gateway-etcd](https://github.com/vlorc/hprose-gateway-consul)
[English](https://github.com/vlorc/hprose-gateway-consul/blob/master/README.md)

[![License](https://img.shields.io/:license-apache-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codebeat badge](https://codebeat.co/badges/c41b426c-4121-4dc8-99c2-f1b60574be64)](https://codebeat.co/projects/github-com-vlorc-hprose-gateway-consul-master)
[![Go Report Card](https://goreportcard.com/badge/github.com/vlorc/hprose-gateway-consul)](https://goreportcard.com/report/github.com/vlorc/hprose-gateway-consul)
[![GoDoc](https://godoc.org/github.com/vlorc/hprose-gateway-consul?status.svg)](https://godoc.org/github.com/vlorc/hprose-gateway-consul)
[![Build Status](https://travis-ci.org/vlorc/hprose-gateway-consul.svg?branch=master)](https://travis-ci.org/vlorc/hprose-gateway-consul?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/vlorc/hprose-gateway-consul/badge.svg?branch=master)](https://coveralls.io/github/vlorc/hprose-gateway-consul?branch=master)

基于golang的hprose网关consul服务发现

## 特性
+ 惰性客户端
+ 服务发现
+ 注册器

## 安装
	go get github.com/vlorc/hprose-gateway-consul

## 快速开始

* 服务解析器
```golang
r := resolver.NewResolver(cli, ctx, "")
// 打印事件
go r.Watch("*", watcher.NewPrintWatcher(fmt.Printf))
```

* 服务注册器
```golang
m := manager.NewManager(cli, context.Background(), "", 5 /*心跳*/)
s := m.Register("user" /*服务名*/, "1" /*ID*/)
s.Update(&types.Service{
	Id:       "1",
	Name:     "user",
	Url:      "http://localhost:8080",
})
```

## 许可证
这个项目是在Apache许可证下进行的。请参阅完整许可证文本的许可证文件。
