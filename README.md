# ftrans

局域网大文件迁移工具：旧机 `send`，新机 `receive`。基于 [iroh](https://www.iroh.computer/)（QUIC + BLAKE3 校验），
支持断点式的失败文件重传。

**默认不依赖任何外部服务器**：不开 relay、不做 n0 DNS 查询、不做 STUN/QAD 探测、不做网关端口映射，
只在局域网内与对端直接通信。

## 用法

发送端（旧机）：

```bash
ftrans send --path /path/to/data        # 可重复 --path，多次发送多个文件/目录
```

它会打印一个六位**会话码**，例如：

```
=== Session code ===
    NA6Q00
```

接收端（新机）只输这个码：

```bash
ftrans receive NA6Q00 --output /path/to/target
```

短码是怎么工作的：发送端在局域网内用 UDP 探测/应答（端口 `53535`）广播「我这儿有一个会话」，
内容包括六位码的**哈希**、本机 endpoint id 和直连地址；接收端凭码找到发送端后，在**加密的 QUIC
连接里**用 `ftrans-meta` 协议把码交出去，换回真正的票据。码本身从不明文广播，也不需要任何服务器，
更不依赖 mDNS/组播（所以校园网封组播也能用）。

- 找不到发送端时：`ftrans receive NA6Q00 --addr 192.168.137.1`（跳过广播，直接探测该地址）。
- 不带参数运行 `ftrans receive` 会列出当前发现到的发送端（主机名 + 地址），方便确认对端在不在。
- 跨网络/走 relay 时仍然可以直接粘完整票据：`ftrans receive "<ticket>"`。

常用参数：

| 参数 | 位置 | 说明 |
| --- | --- | --- |
| `--relay none` | 全局（默认） | 纯局域网，不使用任何外部服务器 |
| `--relay n0` | 全局 | n0 公网 relay，需要能访问 `*.iroh.link`，仅用于跨网络 |
| `--relay <URL>` | 全局 | 自建 relay，例如 `http://10.0.0.5:3340` |
| `--addr <IP>` | receive | 不广播，直接探测指定地址（可重复） |
| `--parallel N` | receive | 并行下载流数量（默认 8） |
| `--strip-root` | receive | 不额外创建源目录这一层 |
| `--no-verify` | receive | 跳过接收后的 BLAKE3 校验 |
| `--retry-failed` | receive | 只重传上次失败的文件（读 `<output>/ftrans-failed.txt`） |
| `--store-dir` | 两者 | 临时索引目录，默认取数据所在盘（存哈希，不占额外空间） |

## 校园网/受限网络

`croc` 之类工具会报 `no public relay server`，因为校园网通常屏蔽境外 DNS 与 relay；
ftrans 默认模式不需要它们，直接在两台机器的内网地址之间传。

按可能性排序的排查项：

1. **Windows 防火墙**：默认 `BlockInbound`，且校园 Wi-Fi 常被识别为"公用网络"。
   首次运行 sender 时放行 `ftrans.exe` 的入站 UDP；`send` 启动时会打印 `Listening on:`，
   确认里面是内网地址而不是 `<none>`。
2. **是否同网段**：两台机器的 IP 应在同一子网（如都在 `10.131.x.x/17`）。
3. **客户端隔离（AP isolation）**：很多校园 Wi-Fi 会禁止终端之间互相通信，表现是
   `ping` 都不通。这一层无法用任何软件绕过，只能改用直连：
   - 用其中一台开热点（Windows：移动热点；Linux：`nmcli device wifi hotspot`），另一台连上，
     两台机器会落在同一个 `192.168.x.x` 网段，再跑 `ftrans` 即可；
   - 或者用网线直连，手动给两端配同网段 IP。

### 本地代理会劫持内网流量（很容易误判成防火墙问题）

若机器上跑着 Clash/V2Ray 之类代理并设置了 `http_proxy`/`https_proxy` 环境变量，而
`no_proxy` 里只有 `localhost,127.0.0.1`，那么**所有走 HTTP 的工具**（curl、LocalSend、
下载器等）访问内网地址时都会把请求发给代理，表现为连接超时，看起来就像被防火墙挡了。

排查时务必绕过代理再测一次：

```bash
curl --noproxy '*' http://<对端内网IP>:<port>/
```

建议在代理软件里把 `10.0.0.0/8`、`172.16.0.0/12`、`192.168.0.0/16`、`127.0.0.1`、
`localhost` 全部加入直连/绕过列表。

ftrans 本身不受影响：它走 QUIC/UDP，不读任何代理环境变量。

传输失败时 ftrans 会打印上述提示；`--relay n0` 模式下等待 relay 最多 10 秒，超时会告警但
局域网直连仍然可用（不会再像以前那样卡死）。

## 在另一台机器上获得 ftrans

```bash
git clone https://github.com/dynamder/ftrans.git
cd ftrans && cargo build --release
```

没有外网时也可以从另一台机器直接拷贝源码（例如 Windows 上 `python -m http.server`，
Linux 上 `curl --noproxy '*' -O`；U 盘同理）。校园网可直连 `static.crates.io`，
拉依赖慢的话可换 `rsproxy.cn`/`ustc` 的 crates.io 镜像。
