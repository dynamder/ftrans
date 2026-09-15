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

接收端（新机）：

```bash
ftrans receive "<ticket>" --output /path/to/target
```

接收端会用票据里携带的地址直连发送端；mDNS 只是额外兜底，即使校园网封了组播也能工作。

常用参数：

| 参数 | 位置 | 说明 |
| --- | --- | --- |
| `--relay none` | 全局（默认） | 纯局域网，不使用任何外部服务器 |
| `--relay n0` | 全局 | n0 公网 relay，需要能访问 `*.iroh.link`，仅用于跨网络 |
| `--relay <URL>` | 全局 | 自建 relay，例如 `http://10.0.0.5:3340` |
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

传输失败时 ftrans 会打印上述提示；`--relay n0` 模式下等待 relay 最多 10 秒，超时会告警但
局域网直连仍然可用（不会再像以前那样卡死）。

## 在另一台机器上获得 ftrans

新机（CachyOS）需要自己编译（校园网可直连 `static.crates.io`，慢的话可换国内镜像
`rsproxy.cn`/`ustc` 的 crates.io 源）：

```bash
cargo build --release --offline   # 依赖已缓存时
cargo build --release             # 需要拉取依赖时
```

源码本身还没有 ftrans 可用时，先用系统自带手段搬一次（例如 Windows 上
`python -m http.server`，Linux 上 `curl -O`；U 盘同理）。
