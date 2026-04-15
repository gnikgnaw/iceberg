# Apache Iceberg FileIO 抽象与多云存储实现深度解析

> 基于 Apache Iceberg 源码的深度分析报告
> 分析日期：2026/04/15

---

## 目录

1. [概述与设计哲学](#1-概述与设计哲学)
2. [FileIO 核心接口体系](#2-fileio-核心接口体系)
   - 2.1 [FileIO 接口](#21-fileio-接口)
   - 2.2 [InputFile 接口](#22-inputfile-接口)
   - 2.3 [OutputFile 接口](#23-outputfile-接口)
   - 2.4 [PositionOutputStream 抽象类](#24-positionoutputstream-抽象类)
   - 2.5 [SeekableInputStream 抽象类](#25-seekableinputstream-抽象类)
   - 2.6 [RangeReadable 接口](#26-rangereadable-接口)
   - 2.7 [扩展接口体系](#27-扩展接口体系)
3. [HadoopFileIO：基于 Hadoop FileSystem 的实现](#3-hadoopfileio基于-hadoop-filesystem-的实现)
   - 3.1 [核心实现分析](#31-核心实现分析)
   - 3.2 [HadoopInputFile 详解](#32-hadoopinputfile-详解)
   - 3.3 [配置传递与序列化](#33-配置传递与序列化)
   - 3.4 [批量删除与并发控制](#34-批量删除与并发控制)
   - 3.5 [配置参数清单](#35-配置参数清单)
4. [S3FileIO：AWS S3 实现](#4-s3fileioaws-s3-实现)
   - 4.1 [整体架构](#41-整体架构)
   - 4.2 [PrefixedS3Client 前缀路由机制](#42-prefixeds3client-前缀路由机制)
   - 4.3 [S3InputStream 深度分析](#43-s3inputstream-深度分析)
   - 4.4 [S3OutputStream 多段上传详解](#44-s3outputstream-多段上传详解)
   - 4.5 [SSE 服务端加密支持](#45-sse-服务端加密支持)
   - 4.6 [Access Point 支持](#46-access-point-支持)
   - 4.7 [凭证刷新机制](#47-凭证刷新机制)
   - 4.8 [文件恢复操作](#48-文件恢复操作)
   - 4.9 [批量删除优化](#49-批量删除优化)
   - 4.10 [配置参数完整清单](#410-配置参数完整清单)
5. [GCSFileIO：Google Cloud Storage 实现](#5-gcsfileiogoogle-cloud-storage-实现)
   - 5.1 [整体架构](#51-整体架构)
   - 5.2 [GCSInputStream 流式读取](#52-gcsinputstream-流式读取)
   - 5.3 [GCSOutputStream 流式写入](#53-gcsoutputstream-流式写入)
   - 5.4 [OAuth2 认证与凭证刷新](#54-oauth2-认证与凭证刷新)
   - 5.5 [Analytics Core 加速库集成](#55-analytics-core-加速库集成)
   - 5.6 [配置参数完整清单](#56-配置参数完整清单)
6. [ADLSFileIO：Azure ADLS Gen2 实现](#6-adlsfileioazure-adls-gen2-实现)
   - 6.1 [整体架构](#61-整体架构)
   - 6.2 [ADLSLocation 路径解析](#62-adlslocation-路径解析)
   - 6.3 [ADLSInputStream 范围读取](#63-adlsinputstream-范围读取)
   - 6.4 [ADLSOutputStream 输出流](#64-adlsoutputstream-输出流)
   - 6.5 [多种认证方式](#65-多种认证方式)
   - 6.6 [Vended Credentials 支持](#66-vended-credentials-支持)
   - 6.7 [配置参数完整清单](#67-配置参数完整清单)
7. [ResolvingFileIO：动态路由实现](#7-resolvingfileio动态路由实现)
   - 7.1 [设计动机](#71-设计动机)
   - 7.2 [URI Scheme 路由机制](#72-uri-scheme-路由机制)
   - 7.3 [实例缓存与生命周期管理](#73-实例缓存与生命周期管理)
   - 7.4 [Fallback 机制](#74-fallback-机制)
   - 7.5 [Storage Credentials 传递](#75-storage-credentials-传递)
8. [EncryptingFileIO：加密层封装](#8-encryptingfileio加密层封装)
   - 8.1 [信封加密模式](#81-信封加密模式)
   - 8.2 [EncryptionManager 接口](#82-encryptionmanager-接口)
   - 8.3 [透明加解密流程](#83-透明加解密流程)
   - 8.4 [与 SupportsPrefixOperations 的兼容](#84-与-supportsprefixoperations-的兼容)
9. [FileIO 的序列化机制](#9-fileio-的序列化机制)
   - 9.1 [SerializableTable 中的 FileIO](#91-serializabletable-中的-fileio)
   - 9.2 [Hadoop Configuration 序列化](#92-hadoop-configuration-序列化)
   - 9.3 [HadoopConfigurable 接口](#93-hadoopconfigurable-接口)
   - 9.4 [FileIOParser JSON 序列化](#94-fileioparser-json-序列化)
   - 9.5 [Kryo 序列化兼容性](#95-kryo-序列化兼容性)
10. [凭证管理与刷新机制](#10-凭证管理与刷新机制)
    - 10.1 [StorageCredential 体系](#101-storagecredential-体系)
    - 10.2 [SupportsStorageCredentials 接口](#102-supportsstoragecredentials-接口)
    - 10.3 [VendedCredentialsProvider (S3)](#103-vendedcredentialsprovider-s3)
    - 10.4 [OAuth2RefreshCredentialsHandler (GCS)](#104-oauth2refreshcredentialshandler-gcs)
    - 10.5 [VendedAdlsCredentialProvider (Azure)](#105-vendedadlscredentialprovider-azure)
11. [ContentCache 文件内容缓存](#11-contentcache-文件内容缓存)
    - 11.1 [缓存设计](#111-缓存设计)
    - 11.2 [CachingInputFile 实现](#112-cachinginputfile-实现)
12. [FileIO 加载机制：CatalogUtil.loadFileIO()](#12-fileio-加载机制catalogutilloadfileio)
    - 12.1 [动态加载流程](#121-动态加载流程)
    - 12.2 [Hadoop 配置注入](#122-hadoop-配置注入)
    - 12.3 [Storage Credentials 注入](#123-storage-credentials-注入)
13. [性能优化与调优指南](#13-性能优化与调优指南)
    - 13.1 [连接池与并发配置](#131-连接池与并发配置)
    - 13.2 [预取与缓冲区优化](#132-预取与缓冲区优化)
    - 13.3 [S3 专项调优](#133-s3-专项调优)
    - 13.4 [GCS 专项调优](#134-gcs-专项调优)
    - 13.5 [ADLS 专项调优](#135-adls-专项调优)
14. [多云场景下的 FileIO 选型策略](#14-多云场景下的-fileio-选型策略)
    - 14.1 [单云部署方案](#141-单云部署方案)
    - 14.2 [混合云/多云部署](#142-混合云多云部署)
    - 14.3 [选型决策树](#143-选型决策树)
    - 14.4 [迁移策略](#144-迁移策略)
15. [总结](#15-总结)

---

## 1. 概述与设计哲学

Apache Iceberg 作为一个开放的表格式，需要在异构存储环境中工作。无论底层是 HDFS、Amazon S3、Google Cloud Storage（GCS）还是 Azure Data Lake Storage（ADLS），Iceberg 都需要一个统一的、可插拔的文件访问抽象层。这就是 **FileIO** 的设计初衷。

Iceberg 的 FileIO 抽象遵循以下核心设计原则：

1. **存储无关性（Storage Agnostic）**：上层代码（Table 操作、Manifest 读写、数据扫描）完全不关心底层存储的具体实现。所有文件操作都通过 FileIO 接口完成。

2. **可序列化（Serializable）**：在分布式计算引擎（Spark、Flink）中，FileIO 实例需要被序列化后传输到 Worker 节点。FileIO 接口继承了 `java.io.Serializable`。

3. **延迟初始化（Lazy Initialization）**：所有 FileIO 实现都提供无参构造函数，通过 `initialize(Map<String, String> properties)` 方法延迟初始化，以支持动态类加载。

4. **装饰器模式（Decorator Pattern）**：通过 `EncryptingFileIO`、`ResolvingFileIO` 等实现，可以在基础 FileIO 上叠加加密、路由等功能。

5. **面向接口编程**：InputFile、OutputFile、SeekableInputStream、PositionOutputStream 等接口形成了完整的 IO 操作体系。

从代码组织来看，FileIO 的接口定义位于 `api` 模块，核心实现位于 `core` 模块（HadoopFileIO、ResolvingFileIO），各云存储实现分别位于对应的模块中：

```
api/          -> FileIO, InputFile, OutputFile, SeekableInputStream, PositionOutputStream
core/         -> HadoopFileIO, ResolvingFileIO, ContentCache
aws/          -> S3FileIO, S3InputStream, S3OutputStream
gcp/          -> GCSFileIO, GCSInputStream, GCSOutputStream
azure/        -> ADLSFileIO, ADLSInputStream, ADLSOutputStream
```

这种模块化设计使得用户只需引入对应云存储的依赖即可获得相应的 FileIO 支持，避免了不必要的依赖膨胀。

---

## 2. FileIO 核心接口体系

### 2.1 FileIO 接口

`FileIO` 是整个文件 IO 体系的根接口，定义在 `api/src/main/java/org/apache/iceberg/io/FileIO.java`。

```java
// FileIO.java (第37-125行)
public interface FileIO extends Serializable, Closeable {
    InputFile newInputFile(String path);
    default InputFile newInputFile(String path, long length) { return newInputFile(path); }
    default InputFile newInputFile(DataFile file) { ... }
    default InputFile newInputFile(DeleteFile file) { ... }
    default InputFile newInputFile(ManifestFile manifest) { ... }
    default InputFile newInputFile(ManifestListFile manifestList) { ... }
    
    OutputFile newOutputFile(String path);
    void deleteFile(String path);
    
    default Map<String, String> properties() { throw new UnsupportedOperationException(...); }
    default void initialize(Map<String, String> properties) {}
    @Override default void close() {}
}
```

FileIO 接口的关键设计点：

**1. 继承 Serializable 和 Closeable**

这意味着 FileIO 必须是可序列化的（用于分布式计算场景），同时需要支持资源释放。`close()` 方法使用 `default` 实现为空操作，对于不需要释放资源的实现可以不覆盖。

**2. 面向 Iceberg 文件类型的重载方法**

FileIO 为 `DataFile`、`DeleteFile`、`ManifestFile`、`ManifestListFile` 都提供了专门的 `newInputFile` 重载。这些默认实现会检查文件是否加密（通过 `keyMetadata()`），如果文件带有加密元数据则抛出异常，提示用户应该使用 `EncryptingFileIO`：

```java
// FileIO.java (第50-56行)
default InputFile newInputFile(DataFile file) {
    Preconditions.checkArgument(
        file.keyMetadata() == null,
        "Cannot decrypt data file: %s (use EncryptingFileIO)",
        file.location());
    return newInputFile(file.location(), file.fileSizeInBytes());
}
```

这个设计将加密关注点从普通 FileIO 中剥离出来，遵循了单一职责原则。

**3. 延迟初始化协议**

`initialize(Map<String, String> properties)` 方法允许通过配置属性字典初始化 FileIO。这一设计使得 FileIO 可以通过反射（无参构造函数）动态创建，再通过配置初始化，符合 SPI（Service Provider Interface）模式。

**4. properties() 方法**

用于序列化支持。`FileIOParser` 在序列化 FileIO 时需要获取其配置属性，以便在反序列化时重新构建。默认抛出 `UnsupportedOperationException`，要求子类显式实现。

### 2.2 InputFile 接口

`InputFile` 接口（`api/src/main/java/org/apache/iceberg/io/InputFile.java`）定义了文件读取的抽象：

```java
// InputFile.java (第30-61行)
public interface InputFile {
    long getLength();
    SeekableInputStream newStream();
    String location();
    boolean exists();
}
```

InputFile 的设计灵感来自 Parquet 的 InputFile（Javadoc 中明确标注 "This class is based on Parquet's InputFile"），它提供了四个核心能力：

- **getLength()**：返回文件总字节数。这对于计算文件 split、验证数据完整性至关重要。
- **newStream()**：创建一个新的 `SeekableInputStream`，支持随机访问读取。每次调用都应该返回一个独立的流实例。
- **location()**：返回文件的完全限定路径（Fully Qualified URI）。
- **exists()**：检查文件是否存在。

值得注意的是，InputFile 不继承 Serializable。它被设计为瞬态对象（transient object），由可序列化的 FileIO 按需创建。

### 2.3 OutputFile 接口

`OutputFile` 接口（`api/src/main/java/org/apache/iceberg/io/OutputFile.java`）定义了文件写入的抽象：

```java
// OutputFile.java (第30-68行)
public interface OutputFile {
    PositionOutputStream create();
    PositionOutputStream createOrOverwrite();
    String location();
    InputFile toInputFile();
}
```

关键设计点：

- **create() vs createOrOverwrite()**：`create()` 在文件已存在时抛出 `AlreadyExistsException`，而 `createOrOverwrite()` 会覆盖已有文件。这种区分对于 Iceberg 的乐观并发控制非常重要 -- 元数据文件提交时应使用 `create()` 以检测冲突。
- **toInputFile()**：允许将 OutputFile 转换为 InputFile。这在写入完成后立即读取验证的场景中非常有用。

### 2.4 PositionOutputStream 抽象类

```java
// PositionOutputStream.java (第24-43行)
public abstract class PositionOutputStream extends OutputStream {
    public abstract long getPos() throws IOException;
    public long storedLength() throws IOException { return getPos(); }
}
```

`PositionOutputStream` 继承自 `java.io.OutputStream`，增加了位置追踪能力：

- **getPos()**：返回当前写入位置（从流开始的字节偏移量）。这对于 Parquet 和 ORC 这类列式文件格式至关重要，因为它们需要在文件头/尾写入元数据（如 footer offset）。
- **storedLength()**：返回实际存储长度。对于加密流，存储长度可能与逻辑位置不同（因为加密可能引入 padding）。默认实现返回 `getPos()`。

### 2.5 SeekableInputStream 抽象类

```java
// SeekableInputStream.java (第30-46行)
public abstract class SeekableInputStream extends InputStream {
    public abstract long getPos() throws IOException;
    public abstract void seek(long newPos) throws IOException;
}
```

`SeekableInputStream` 继承自 `java.io.InputStream`，增加了随机定位能力。这是列式文件格式读取的基础需求 -- Parquet 文件需要先读取文件尾部的 footer，获取 row group 的偏移量后再跳转到相应位置读取数据。

### 2.6 RangeReadable 接口

`RangeReadable` 接口（`api/src/main/java/org/apache/iceberg/io/RangeReadable.java`）是对 SeekableInputStream 的增强扩展，提供了基于位置的范围读取能力：

```java
// RangeReadable.java (第42-143行)
public interface RangeReadable extends Closeable {
    void readFully(long position, byte[] buffer, int offset, int length) throws IOException;
    default void readFully(long position, byte[] buffer) throws IOException { ... }
    int readTail(byte[] buffer, int offset, int length) throws IOException;
    default int readTail(byte[] buffer) throws IOException { ... }
    default void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate) throws IOException { ... }
}
```

三个关键方法：

- **readFully()**：从指定位置读取精确数量的字节。使用 HTTP Range 请求时非常高效，避免了 seek + read 的两步操作。
- **readTail()**：从文件末尾读取指定长度的字节。这对于 Parquet 文件读取 footer 特别有效率（`bytes=-{length}` 形式的 Range 请求）。
- **readVectored()**：向量化读取，一次性请求多个不连续的文件范围。默认实现串行地调用 `readFully`，但子类可以利用存储系统的批量读取能力进行并行化。

S3InputStream、GCSInputStream、ADLSInputStream 都实现了 RangeReadable 接口，充分利用云存储的 Range 请求优化。

### 2.7 扩展接口体系

#### DelegateFileIO

```java
// DelegateFileIO.java (第25行)
public interface DelegateFileIO extends FileIO, SupportsPrefixOperations, SupportsBulkOperations {}
```

`DelegateFileIO` 是一个组合接口（mixin interface），将 FileIO 与前缀操作和批量操作能力结合在一起。所有需要作为 ResolvingFileIO 委托目标的 FileIO 实现都必须实现此接口。S3FileIO、GCSFileIO、ADLSFileIO、HadoopFileIO 全部实现了 DelegateFileIO。

#### SupportsPrefixOperations

```java
// SupportsPrefixOperations.java (第25-47行)
public interface SupportsPrefixOperations extends FileIO {
    Iterable<FileInfo> listPrefix(String prefix);
    void deletePrefix(String prefix);
}
```

提供基于前缀的文件列表和删除能力。对于对象存储（S3/GCS），prefix 可以是任意字符串前缀；对于层次化文件系统（HDFS），prefix 必须是完整的目录路径。

#### SupportsBulkOperations

```java
// SupportsBulkOperations.java (第21-29行)
public interface SupportsBulkOperations extends FileIO {
    void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException;
}
```

批量删除能力。S3 支持单次请求删除最多 1000 个对象（`DeleteObjects` API），GCS 支持批量操作，这比逐个删除效率高出数个数量级。

#### SupportsRecoveryOperations

```java
// SupportsRecoveryOperations.java (第27-36行)
public interface SupportsRecoveryOperations {
    boolean recoverFile(String path);
}
```

文件恢复能力。目前只有 S3FileIO 实现了此接口，利用 S3 对象版本控制来恢复被删除的文件。

#### SupportsStorageCredentials

```java
// SupportsStorageCredentials.java (第27-32行)
public interface SupportsStorageCredentials {
    void setCredentials(List<StorageCredential> credentials);
    List<StorageCredential> credentials();
}
```

存储凭证管理能力。支持从外部（如 REST Catalog）接收和更新存储凭证。S3FileIO、GCSFileIO、ResolvingFileIO 都实现了此接口。

---

## 3. HadoopFileIO：基于 Hadoop FileSystem 的实现

### 3.1 核心实现分析

`HadoopFileIO` 位于 `core/src/main/java/org/apache/iceberg/hadoop/HadoopFileIO.java`，是 Iceberg 内置的、基于 Hadoop FileSystem API 的 FileIO 实现。

```java
// HadoopFileIO.java (第48-49行)
public class HadoopFileIO implements HadoopConfigurable, DelegateFileIO {
    private volatile SerializableSupplier<Configuration> hadoopConf;
    private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());
}
```

HadoopFileIO 同时实现了 `HadoopConfigurable` 和 `DelegateFileIO`。由于 Hadoop `Configuration` 对象本身不可序列化，HadoopFileIO 通过 `SerializableSupplier<Configuration>` 来持有配置引用。

核心方法实现非常直接，直接委托给 Hadoop 的 FileSystem API：

```java
// HadoopFileIO.java (第91-113行)
@Override
public InputFile newInputFile(String path) {
    return HadoopInputFile.fromLocation(path, getConf());
}

@Override
public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), getConf());
}

@Override
public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFs(toDelete, getConf());
    try {
        fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
}
```

### 3.2 HadoopInputFile 详解

`HadoopInputFile`（`core/src/main/java/org/apache/iceberg/hadoop/HadoopInputFile.java`）是 InputFile 接口的 Hadoop 实现。它还实现了 `NativelyEncryptedFile` 接口，支持原生文件格式加密参数传递。

关键实现细节：

**1. 延迟获取 FileStatus**

```java
// HadoopInputFile.java (第159-170行)
private FileStatus lazyStat() {
    if (stat == null) {
        try {
            this.stat = fs.getFileStatus(path);
        } catch (FileNotFoundException e) {
            throw new NotFoundException(e, "File does not exist: %s", path);
        } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to get status for file: %s", path);
        }
    }
    return stat;
}
```

FileStatus 是通过懒加载获取的，如果已经知道文件长度（在 `newInputFile(path, length)` 中传入），可以完全避免这次远程调用。

**2. Block 位置信息**

```java
// HadoopInputFile.java (第207-219行)
public String[] getBlockLocations(long start, long end) {
    List<String> hosts = Lists.newArrayList();
    try {
        for (BlockLocation bl : fs.getFileBlockLocations(path, start, end)) {
            Collections.addAll(hosts, bl.getHosts());
        }
        return hosts.toArray(NO_LOCATION_PREFERENCE);
    } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to get block locations for path: %s", path);
    }
}
```

HadoopInputFile 独有的 `getBlockLocations` 方法提供了数据本地性（data locality）信息。对于 HDFS 部署，Spark 可以利用此信息将计算任务调度到数据所在的节点上。

**3. 流创建**

```java
// HadoopInputFile.java (第181-189行)
@Override
public SeekableInputStream newStream() {
    try {
        return HadoopStreams.wrap(fs.open(path));
    } catch (FileNotFoundException e) {
        throw new NotFoundException(e, "Failed to open input stream for file: %s", path);
    } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to open input stream for file: %s", path);
    }
}
```

通过 `HadoopStreams.wrap` 将 Hadoop 的 `FSDataInputStream` 包装为 Iceberg 的 `SeekableInputStream`。

### 3.3 配置传递与序列化

HadoopFileIO 的配置序列化是通过 `SerializableConfiguration` 实现的：

```java
// SerializableConfiguration.java (第28-51行)
public class SerializableConfiguration implements SerializableSupplier<Configuration> {
    private final Map<String, String> confAsMap;
    private transient volatile Configuration hadoopConf = null;

    public SerializableConfiguration(Configuration hadoopConf) {
        this.confAsMap = Maps.newHashMapWithExpectedSize(hadoopConf.size());
        hadoopConf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public Configuration get() {
        if (hadoopConf == null) {
            synchronized (this) {
                if (hadoopConf == null) {
                    Configuration newConf = new Configuration(false);
                    confAsMap.forEach(newConf::set);
                    this.hadoopConf = newConf;
                }
            }
        }
        return hadoopConf;
    }
}
```

**关键实现细节**：

1. 序列化时将 Hadoop Configuration 转换为 `Map<String, String>` 存储
2. Configuration 对象声明为 `transient volatile`，反序列化后需要从 Map 重建
3. 使用 `new Configuration(false)` 避免加载默认配置，只使用显式设置的属性
4. 采用 DCL（Double-Checked Locking）模式确保线程安全的懒加载

`HadoopFileIO.getConf()` 方法还有一个防御性设计：

```java
// HadoopFileIO.java (第127-139行)
@Override
public Configuration getConf() {
    if (hadoopConf == null) {
        synchronized (this) {
            if (hadoopConf == null) {
                this.hadoopConf = new SerializableConfiguration(new Configuration());
            }
        }
    }
    return hadoopConf.get();
}
```

如果 Configuration 未被设置（比如通过反射创建但未调用 `setConf`），会自动创建一个默认的 Configuration，防止 NPE。

### 3.4 批量删除与并发控制

```java
// HadoopFileIO.java (第186-203行)
@Override
public void deleteFiles(Iterable<String> pathsToDelete) throws BulkDeletionFailureException {
    AtomicInteger failureCount = new AtomicInteger(0);
    Tasks.foreach(pathsToDelete)
        .executeWith(executorService())
        .retry(DELETE_RETRY_ATTEMPTS)
        .stopRetryOn(FileNotFoundException.class)
        .suppressFailureWhenFinished()
        .onFailure((f, e) -> {
            LOG.error("Failure during bulk delete on file: {} ", f, e);
            failureCount.incrementAndGet();
        })
        .run(this::deleteFile);

    if (failureCount.get() != 0) {
        throw new BulkDeletionFailureException(failureCount.get());
    }
}
```

由于 HDFS 不像 S3 那样支持批量删除 API，HadoopFileIO 使用线程池并发执行单文件删除操作。关键策略：

1. 删除操作重试 3 次（`DELETE_RETRY_ATTEMPTS = 3`）
2. 如果文件不存在（`FileNotFoundException`），停止重试
3. 使用静态共享线程池，线程数默认为 `CPU核心数 * 4`
4. 所有失败的删除操作会被汇总报告

### 3.5 配置参数清单

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `iceberg.hadoop.delete-file-parallelism` | 批量删除并发线程数 | `CPU核心数 * 4` |

HadoopFileIO 本身的配置较少，主要依赖 Hadoop Configuration 中的标准属性（如 `fs.defaultFS`、`dfs.replication` 等）。

---

## 4. S3FileIO：AWS S3 实现

### 4.1 整体架构

`S3FileIO`（`aws/src/main/java/org/apache/iceberg/aws/s3/S3FileIO.java`）是 Iceberg 中最复杂、功能最丰富的 FileIO 实现。

```java
// S3FileIO.java (第94-98行)
public class S3FileIO
    implements CredentialSupplier,
        DelegateFileIO,
        SupportsRecoveryOperations,
        SupportsStorageCredentials {
```

S3FileIO 实现了五个接口，支持：
- 基本文件操作（DelegateFileIO）
- 凭证供应（CredentialSupplier，已废弃）
- 文件恢复（SupportsRecoveryOperations）
- 存储凭证管理（SupportsStorageCredentials）

S3FileIO 的核心组件包括：

1. **PrefixedS3Client**：基于存储路径前缀分发 S3 客户端的路由组件
2. **S3FileIOProperties**：配置属性管理类
3. **S3URI**：S3 路径解析工具类
4. **S3InputFile / S3OutputFile**：文件级抽象
5. **S3InputStream / S3OutputStream**：流级实现
6. **VendedCredentialsProvider**：凭证刷新机制

### 4.2 PrefixedS3Client 前缀路由机制

S3FileIO 支持基于路径前缀的客户端路由，核心是 `PrefixedS3Client` 和 `clientByPrefix` 映射：

```java
// S3FileIO.java (第387-403行)
PrefixedS3Client clientForStoragePath(String storagePath) {
    PrefixedS3Client client;
    String matchingPrefix = ROOT_PREFIX;

    for (String storagePrefix : clientByPrefix().keySet()) {
        if (storagePath.startsWith(storagePrefix)
            && storagePrefix.length() > matchingPrefix.length()) {
            matchingPrefix = storagePrefix;
        }
    }

    client = clientByPrefix().getOrDefault(matchingPrefix, null);
    Preconditions.checkState(
        null != client, "[BUG] S3 client for storage path not available: %s", storagePath);
    return client;
}
```

这个路由机制的工作方式是**最长前缀匹配**（Longest Prefix Match）：对于给定的存储路径，选择与之匹配的最长前缀对应的客户端。这允许不同的 S3 路径（比如不同 bucket）使用不同的凭证和配置。

`PrefixedS3Client` 自身是一个封装了 S3 同步/异步客户端及其配置的容器：

```java
// PrefixedS3Client.java (第30-123行)
class PrefixedS3Client implements AutoCloseable {
    private final String storagePrefix;
    private final S3FileIOProperties s3FileIOProperties;
    private SerializableSupplier<S3Client> s3;
    private SerializableSupplier<S3AsyncClient> s3Async;
    private transient volatile S3Client s3Client;
    private transient volatile S3AsyncClient s3AsyncClient;
}
```

客户端使用懒加载+DCL模式初始化，支持通过 `S3FileIOAwsClientFactory` 自定义客户端创建逻辑。

### 4.3 S3InputStream 深度分析

`S3InputStream`（`aws/src/main/java/org/apache/iceberg/aws/s3/S3InputStream.java`）是 S3 读取的核心实现。

**继承关系**：`S3InputStream extends SeekableInputStream implements RangeReadable`

**核心状态变量**：

```java
// S3InputStream.java (第63-66行)
private InputStream stream;
private long pos = 0;    // 当前底层流的实际位置
private long next = 0;   // 用户请求的下一个读取位置
private boolean closed = false;
```

`pos` 和 `next` 的分离设计允许 seek 操作延迟执行（lazy seek）。seek 时只更新 `next`，实际的流重定位推迟到下一次 read 时才执行。

**Seek 优化策略**：

```java
// S3InputStream.java (第200-226行)
private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
        return; // 已在正确位置
    }

    if ((stream != null) && (next > pos)) {
        long skip = next - pos;
        if (skip <= Math.max(stream.available(), skipSize)) {
            // 小范围前跳：跳过数据而非重新打开流
            LOG.debug("Read-through seek for {} to offset {}", location, next);
            try {
                ByteStreams.skipFully(stream, skip);
                pos = next;
                return;
            } catch (IOException ignored) { }
        }
    }

    // 大范围跳转：关闭当前流，在新位置打开新流
    pos = next;
    openStream();
}
```

这个三层 seek 优化策略：
1. **零开销**：如果 `next == pos`，无需任何操作
2. **Read-through**：如果前跳距离小于缓冲区大小或 `skipSize`（默认 1MB），直接跳过数据
3. **重新打开**：对于大范围跳转，关闭当前 HTTP 连接，使用 Range 请求从新位置打开

**Range 读取**：

```java
// S3InputStream.java (第184-191行)
private InputStream readRange(String range) {
    GetObjectRequest.Builder requestBuilder =
        GetObjectRequest.builder().bucket(location.bucket()).key(location.key()).range(range);
    S3RequestUtil.configureEncryption(s3FileIOProperties, requestBuilder);
    return s3.getObject(requestBuilder.build(), ResponseTransformer.toInputStream());
}
```

Range 请求支持 `readFully(position, buffer, offset, length)` 和 `readTail(buffer, offset, length)` 两种模式，后者使用 `bytes=-{length}` 形式从文件末尾读取。

**错误重试机制**：

```java
// S3InputStream.java (第72-87行)
private RetryPolicy<Object> retryPolicy =
    RetryPolicy.builder()
        .handle(RETRYABLE_EXCEPTIONS) // SSLException, SocketTimeoutException, SocketException
        .onRetry(e -> {
            LOG.warn("Retrying read from S3, reopening stream (attempt {})", e.getAttemptCount());
            resetForRetry();
        })
        .onFailure(e -> LOG.error("Failed to read from S3 input stream after exhausting all retries", e.getException()))
        .withMaxRetries(3)
        .build();
```

使用 Failsafe 库实现自动重试，对网络相关异常（SSL、Socket Timeout、Socket Error）最多重试 3 次。每次重试时重新打开 HTTP 流。

### 4.4 S3OutputStream 多段上传详解

`S3OutputStream`（`aws/src/main/java/org/apache/iceberg/aws/s3/S3OutputStream.java`）实现了 S3 的智能上传策略。

**上传策略选择**：

S3OutputStream 根据数据量大小自动选择上传方式：

1. **小文件：PutObject 单次上传** -- 当总数据量小于 `multiPartThresholdSize`（默认 `32MB * 1.5 = 48MB`）时
2. **大文件：Multipart Upload 多段上传** -- 当数据量超过阈值时

```java
// S3OutputStream.java (第164-180行)
@Override
public void write(int b) throws IOException {
    if (stream.getCount() >= multiPartSize) {
        newStream();
        uploadParts();
    }
    stream.write(b);
    pos += 1;
    
    // 达到阈值后切换到多段上传
    if (multipartUploadId == null && pos >= multiPartThresholdSize) {
        initializeMultiPartUpload();
        uploadParts();
    }
}
```

**多段上传流程**：

1. **分段写入暂存文件**：数据先写入本地暂存目录（`s3.staging-dir`，默认 `java.io.tmpdir`）的临时文件
2. **并行上传**：当一个分段写满（默认 32MB），立即在后台线程池中异步上传
3. **MD5 校验**：可选的数据完整性校验，通过 `DigestOutputStream` 计算每个分段和完整文件的 MD5
4. **完成上传**：所有分段上传完成后，发送 `CompleteMultipartUpload` 请求

```java
// S3OutputStream.java (第293-352行)
private void uploadParts() {
    if (multipartUploadId == null) return;
    
    stagingFiles.stream()
        .filter(f -> closed || !f.file().equals(currentStagingFile))
        .filter(Predicates.not(f -> multiPartMap.containsKey(f.file())))
        .forEach(fileAndDigest -> {
            // 构建 UploadPartRequest
            UploadPartRequest.Builder requestBuilder = UploadPartRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .uploadId(multipartUploadId)
                .partNumber(stagingFiles.indexOf(fileAndDigest) + 1)
                .contentLength(f.length());

            // 异步上传
            CompletableFuture<CompletedPart> future =
                CompletableFuture.supplyAsync(() -> {
                    UploadPartResponse response = s3.uploadPart(uploadRequest, RequestBody.fromFile(f));
                    return CompletedPart.builder().eTag(response.eTag()).partNumber(uploadRequest.partNumber()).build();
                }, executorService)
                .whenComplete((result, thrown) -> {
                    Files.deleteIfExists(f.toPath()); // 上传后删除暂存文件
                });

            multiPartMap.put(f, future);
        });
}
```

**失败处理**：

如果上传过程中发生错误，S3OutputStream 会调用 `abortUpload()` 发送 `AbortMultipartUpload` 请求，清理已上传的分段，并删除所有本地暂存文件。

### 4.5 SSE 服务端加密支持

S3FileIO 通过 `S3RequestUtil` 工具类支持四种 SSE 加密方式：

```java
// S3RequestUtil.java (第97-134行)
static void configureEncryption(S3FileIOProperties s3FileIOProperties, ...) {
    switch (s3FileIOProperties.sseType().toLowerCase(Locale.ENGLISH)) {
        case S3FileIOProperties.SSE_TYPE_NONE:      // 无加密
            break;
        case S3FileIOProperties.SSE_TYPE_KMS:        // SSE-KMS
            encryptionSetter.apply(ServerSideEncryption.AWS_KMS);
            kmsKeySetter.apply(s3FileIOProperties.sseKey());
            break;
        case S3FileIOProperties.DSSE_TYPE_KMS:       // DSSE-KMS（双层加密）
            encryptionSetter.apply(ServerSideEncryption.AWS_KMS_DSSE);
            kmsKeySetter.apply(s3FileIOProperties.sseKey());
            break;
        case S3FileIOProperties.SSE_TYPE_S3:         // SSE-S3
            encryptionSetter.apply(ServerSideEncryption.AES256);
            break;
        case S3FileIOProperties.SSE_TYPE_CUSTOM:     // SSE-C（客户管理密钥）
            customAlgorithmSetter.apply(ServerSideEncryption.AES256.name());
            customKeySetter.apply(s3FileIOProperties.sseKey());
            customMd5Setter.apply(s3FileIOProperties.sseMd5());
            break;
    }
}
```

| SSE 类型 | 配置值 | 说明 |
|---------|-------|------|
| 无加密 | `none` | 不使用服务端加密 |
| SSE-KMS | `kms` | 使用 AWS KMS 管理的密钥加密 |
| DSSE-KMS | `dsse-kms` | 双层 KMS 加密（合规场景） |
| SSE-S3 | `s3` | 使用 S3 管理的密钥（AES-256） |
| SSE-C | `custom` | 使用客户提供的加密密钥 |

SSE-C 模式需要额外提供 `s3.sse.key`（AES256 密钥的 Base64 编码）和 `s3.sse.md5`（密钥的 MD5 摘要）。值得注意的是，SSE-C 加密参数需要在 `HeadObject`、`GetObject`、`PutObject`、`UploadPart` 等所有请求中都携带。

### 4.6 Access Point 支持

S3FileIO 支持 Access Point，通过 `s3.access-points.` 前缀配置 bucket 到 Access Point 的映射：

```java
// S3FileIOProperties.java (第456行)
public static final String ACCESS_POINTS_PREFIX = "s3.access-points.";
```

在 S3URI 解析时，bucket 名称会被自动替换为 Access Point ARN：

```java
// S3URI.java (第70-84行)
S3URI(String location, Map<String, String> bucketToAccessPointMapping) {
    // ...
    this.bucket = bucketToAccessPointMapping == null
        ? authoritySplit[0]
        : bucketToAccessPointMapping.getOrDefault(authoritySplit[0], authoritySplit[0]);
    // ...
}
```

S3 Directory Bucket（Express One Zone）也得到了特殊支持，通过 bucket 名称后缀 `--x-s3` 自动识别。

### 4.7 凭证刷新机制

S3FileIO 支持两层凭证刷新机制：

**1. S3 客户端级别：VendedCredentialsProvider**

`VendedCredentialsProvider` 实现了 AWS SDK 的 `AwsCredentialsProvider` 接口，通过 REST API 从 Catalog 服务获取和刷新凭证：

```java
// VendedCredentialsProvider.java (第48-196行)
public class VendedCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {
    private final CachedSupplier<AwsCredentials> credentialCache;
    
    private RefreshResult<AwsCredentials> refreshCredential() {
        LoadCredentialsResponse response = fetchCredentials();
        // 解析 S3 凭证...
        Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(tokenExpiresAtMillis));
        Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);
        
        return RefreshResult.builder(credentials)
            .staleTime(expiresAt)
            .prefetchTime(prefetchAt)
            .build();
    }
}
```

使用 AWS SDK 的 `CachedSupplier` 自动管理凭证缓存和过期前预刷新（提前 5 分钟刷新）。

**2. FileIO 级别：StorageCredential 刷新**

S3FileIO 还支持通过 `SupportsStorageCredentials` 接口接收外部凭证，并通过定时器自动刷新：

```java
// S3FileIO.java (第441-457行)
private void scheduleCredentialRefresh() {
    storageCredentials.stream()
        .map(storageCredential ->
            storageCredential.config().get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS))
        .filter(Objects::nonNull)
        .map(expiresAtString -> Instant.ofEpochMilli(Long.parseLong(expiresAtString)))
        .min(Comparator.naturalOrder())
        .ifPresent(expiresAt -> {
            Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);
            long delay = Duration.between(Instant.now(), prefetchAt).toMillis();
            this.refreshFuture = executorService()
                .schedule(this::refreshStorageCredentials, delay, TimeUnit.MILLISECONDS);
        });
}
```

当收到 StorageCredential 后，S3FileIO 会计算最早的过期时间，并在过期前 5 分钟调度刷新任务。刷新时通过 `VendedCredentialsProvider` 从 Catalog 重新获取凭证，然后重建 S3 客户端。

### 4.8 文件恢复操作

S3FileIO 实现了 `SupportsRecoveryOperations` 接口，利用 S3 对象版本控制来恢复被删除的文件：

```java
// S3FileIO.java (第567-612行)
@Override
public boolean recoverFile(String path) {
    // 列出对象的所有版本
    ListObjectVersionsIterable response = client.s3().listObjectVersionsPaginator(
        builder -> builder.bucket(location.bucket()).prefix(location.key()));

    // 找到最后修改的版本（非删除标记）
    Optional<ObjectVersion> recoverVersion =
        response.versions().stream().max(Comparator.comparing(ObjectVersion::lastModified));

    return recoverVersion
        .map(version -> recoverObject(client, version, location.bucket()))
        .orElse(false);
}

private boolean recoverObject(PrefixedS3Client client, ObjectVersion version, String bucket) {
    if (version.isLatest()) return true;
    
    // 通过 copyObject 恢复，而非删除 Delete Marker
    // 这样不需要 delete 权限
    client.s3().copyObject(builder -> builder
        .sourceBucket(bucket)
        .sourceKey(version.key())
        .sourceVersionId(version.versionId())
        .destinationBucket(bucket)
        .destinationKey(version.key()));
    return true;
}
```

设计上选择使用 `copyObject` 而非删除 Delete Marker 来恢复文件，这样操作只需要 `s3:GetObject` 和 `s3:PutObject` 权限，不需要 `s3:DeleteObject` 权限。

### 4.9 批量删除优化

S3FileIO 的批量删除实现非常精细：

```java
// S3FileIO.java (第202-272行)
@Override
public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    // 1. 如果配置了 delete tags，先标记文件
    if (s3FileIOProperties.deleteTags() != null && !s3FileIOProperties.deleteTags().isEmpty()) {
        Tasks.foreach(paths).executeWith(executorService())
            .run(path -> tagFileToDelete(clientForStoragePath(path), path, s3FileIOProperties.deleteTags()));
    }

    // 2. 按 bucket 分组，达到批量大小时提交删除
    if (s3FileIOProperties.isDeleteEnabled()) {
        SetMultimap<String, String> bucketToObjects = Multimaps.newSetMultimap(...);
        List<Future<List<String>>> deletionTasks = Lists.newArrayList();
        
        for (String path : paths) {
            // 按 bucket 积累 key
            bucketToObjects.get(bucket).add(objectKey);
            if (bucketToObjects.get(bucket).size() == client.s3FileIOProperties().deleteBatchSize()) {
                // 批量大小达到阈值，异步提交删除任务
                Future<List<String>> deletionTask =
                    executorService().submit(() -> deleteBatch(client, bucket, keys));
                deletionTasks.add(deletionTask);
                bucketToObjects.removeAll(bucket);
            }
        }
        
        // 3. 删除剩余的文件
        for (...) {
            deletionTasks.add(executorService().submit(() -> deleteBatch(...)));
        }
        
        // 4. 等待所有删除完成并汇总失败
        int totalFailedDeletions = 0;
        for (Future<List<String>> deletionTask : deletionTasks) {
            List<String> failedDeletions = deletionTask.get();
            totalFailedDeletions += failedDeletions.size();
        }
    }
}
```

关键优化点：
1. 按 bucket 分组，每批最多 250 个对象（可配置，最大 1000）
2. 异步并行提交删除任务
3. 支持 "软删除" 模式：只打标签不真正删除，配合 S3 Lifecycle Policy 自动归档/删除
4. 支持禁用实际删除（`s3.delete-enabled=false`）

### 4.10 配置参数完整清单

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `s3.client-factory-impl` | S3 客户端工厂实现类 | - |
| `s3.access-grants.enabled` | 启用 S3 Access Grants | `false` |
| `s3.access-grants.fallback-to-iam` | Access Grants 回退到 IAM | `false` |
| `s3.analytics-accelerator.enabled` | 启用 S3 Analytics Accelerator | `false` |
| `s3.crt.enabled` | 使用 S3 CRT 异步客户端 | `true` |
| `s3.crt.max-concurrency` | CRT 客户端最大并发数 | `500` |
| `s3.sse.type` | 服务端加密类型 | `none` |
| `s3.sse.key` | 加密密钥（KMS Key ID/ARN 或 SSE-C 密钥） | - |
| `s3.sse.md5` | SSE-C 密钥的 MD5 摘要 | - |
| `s3.multipart.num-threads` | 多段上传线程数 | `CPU核心数` |
| `s3.multipart.part-size-bytes` | 多段上传分段大小 | `32MB` |
| `s3.multipart.threshold` | 多段上传触发阈值因子 | `1.5` |
| `s3.staging-dir` | 上传暂存目录 | `java.io.tmpdir` |
| `s3.acl` | 写入时的 Canned ACL | - |
| `s3.endpoint` | 自定义 S3 端点 | - |
| `s3.path-style-access` | 使用 Path-Style 访问 | `false` |
| `s3.access-key-id` | 静态 Access Key ID | - |
| `s3.secret-access-key` | 静态 Secret Access Key | - |
| `s3.session-token` | 临时会话 Token | - |
| `s3.session-token-expires-at-ms` | 会话 Token 过期时间戳 | - |
| `s3.use-arn-region-enabled` | 启用 ARN Region | `false` |
| `s3.checksum-enabled` | 启用 eTag 校验 | `false` |
| `s3.remote-signing-enabled` | 启用远程签名 | `false` |
| `s3.chunked-encoding-enabled` | 启用分块编码 | `true` |
| `s3.delete.batch-size` | 批量删除大小 | `250` |
| `s3.write.tags.*` | 写入时添加的对象标签 | - |
| `s3.write.table-tag-enabled` | 写入时自动添加表名标签 | `false` |
| `s3.write.namespace-tag-enabled` | 写入时自动添加命名空间标签 | `false` |
| `s3.write.storage-class` | 写入存储类别 | - |
| `s3.delete.tags.*` | 删除时添加的标签 | - |
| `s3.delete.num-threads` | 删除标签线程数 | `CPU核心数` |
| `s3.delete-enabled` | 是否实际执行删除 | `true` |
| `s3.acceleration-enabled` | 启用传输加速 | `false` |
| `s3.dualstack-enabled` | 启用双栈端点 | `false` |
| `s3.cross-region-access-enabled` | 启用跨区域访问 | `false` |
| `s3.access-points.*` | Bucket 到 Access Point 的映射 | - |
| `s3.preload-client-enabled` | 初始化时预加载客户端 | `false` |
| `s3.retry.num-retries` | S3 操作重试次数 | `5` |
| `s3.retry.min-wait-ms` | 最小重试等待时间 | `2000` |
| `s3.retry.max-wait-ms` | 最大重试等待时间 | `20000` |
| `s3.directory-bucket.list-prefix-as-directory` | Directory Bucket 列表前缀作为目录 | `true` |

---

## 5. GCSFileIO：Google Cloud Storage 实现

### 5.1 整体架构

`GCSFileIO`（`gcp/src/main/java/org/apache/iceberg/gcp/gcs/GCSFileIO.java`）是基于 Google Cloud Storage Java Client 库实现的 FileIO。

```java
// GCSFileIO.java (第72行)
public class GCSFileIO implements DelegateFileIO, SupportsStorageCredentials {
```

与 S3FileIO 类似，GCSFileIO 也采用了 PrefixedStorage 模式，支持基于路径前缀的存储客户端路由。核心组件包括：

- **PrefixedStorage**：封装了 `Storage` 客户端和 `GCPProperties` 配置
- **GCSLocation**：GCS URI 解析（`gs://bucket/path` 格式）
- **GCSInputFile / GCSOutputFile**：文件级抽象
- **GCSInputStream / GCSOutputStream**：流级实现

### 5.2 GCSInputStream 流式读取

GCSInputStream 使用 GCS 的 `ReadChannel` 原生 API 进行流式读取：

```java
// GCSInputStream.java (第85-104行)
private void openStream() {
    channel = openChannel();
}

private ReadChannel openChannel() {
    List<BlobSourceOption> sourceOptions = Lists.newArrayList();
    gcpProperties.decryptionKey().ifPresent(key -> sourceOptions.add(BlobSourceOption.decryptionKey(key)));
    gcpProperties.userProject().ifPresent(userProject -> sourceOptions.add(BlobSourceOption.userProject(userProject)));
    
    ReadChannel result = storage.reader(blobId, sourceOptions.toArray(new BlobSourceOption[0]));
    gcpProperties.channelReadChunkSize().ifPresent(result::setChunkSize);
    return result;
}
```

**与 S3InputStream 的对比**：

GCSInputStream 的 seek 实现直接委托给 `ReadChannel.seek()`，不像 S3InputStream 那样使用 Read-through 优化：

```java
// GCSInputStream.java (第112-122行)
@Override
public void seek(long newPos) {
    Preconditions.checkState(!closed, "already closed");
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);
    pos = newPos;
    try {
        channel.seek(newPos);
    } catch (IOException e) {
        throw new UncheckedIOException(e);
    }
}
```

**Range 读取优化**：

GCSInputStream 实现了 `RangeReadable`，对 `readFully` 和 `readTail` 都使用独立的 ReadChannel 实现位置读取：

```java
// GCSInputStream.java (第154-176行)
@Override
public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    try (ReadChannel readChannel = openChannel()) {
        readChannel.seek(position);
        readChannel.limit(position + length);
        int bytesRead = read(readChannel, ByteBuffer.wrap(buffer), offset, length);
        if (bytesRead < length) {
            throw new EOFException("Reached the end of stream with " + (length - bytesRead) + " bytes left to read");
        }
    }
}

@Override
public int readTail(byte[] buffer, int offset, int length) throws IOException {
    if (blobSize == null) {
        blobSize = storage.get(blobId).getSize();
    }
    long startPosition = Math.max(0, blobSize - length);
    try (ReadChannel readChannel = openChannel()) {
        readChannel.seek(startPosition);
        return read(readChannel, ByteBuffer.wrap(buffer), offset, length);
    }
}
```

注意 `readTail` 需要先获取文件大小（如果未缓存）来计算起始位置。这与 S3InputStream 使用 `bytes=-{length}` 直接读取尾部的方式不同。

### 5.3 GCSOutputStream 流式写入

GCSOutputStream 使用 GCS 的 `WriteChannel` 实现流式上传：

```java
// GCSOutputStream.java (第103-119行)
private void openStream() {
    List<BlobWriteOption> writeOptions = Lists.newArrayList();
    gcpProperties.encryptionKey().ifPresent(key -> writeOptions.add(BlobWriteOption.encryptionKey(key)));
    gcpProperties.userProject().ifPresent(userProject -> writeOptions.add(BlobWriteOption.userProject(userProject)));

    WriteChannel channel = storage.writer(
        BlobInfo.newBuilder(blobId).build(), writeOptions.toArray(new BlobWriteOption[0]));
    gcpProperties.channelWriteChunkSize().ifPresent(channel::setChunkSize);
    stream = Channels.newOutputStream(channel);
}
```

与 S3OutputStream 的复杂多段上传相比，GCSOutputStream 的实现简洁得多。GCS 的 `WriteChannel` 底层已经实现了 Resumable Upload 协议，会自动处理分段上传逻辑。不需要像 S3 那样手动管理暂存文件和分段编号。

### 5.4 OAuth2 认证与凭证刷新

GCSFileIO 支持 OAuth2 Token 认证和自动刷新。凭证通过 `GCPProperties` 传递：

```java
// GCPProperties.java (第50-51行)
public static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
public static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
```

凭证刷新机制与 S3FileIO 类似：

```java
// GCSFileIO.java (第222-238行)
private void scheduleCredentialRefresh() {
    storageCredentials.stream()
        .map(storageCredential ->
            storageCredential.config().get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT))
        .filter(Objects::nonNull)
        .map(expiresAtString -> Instant.ofEpochMilli(Long.parseLong(expiresAtString)))
        .min(Comparator.naturalOrder())
        .ifPresent(expiresAt -> {
            Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);
            long delay = Duration.between(Instant.now(), prefetchAt).toMillis();
            this.refreshFuture = executorService()
                .schedule(this::refreshStorageCredentials, delay, TimeUnit.MILLISECONDS);
        });
}
```

刷新时通过 `OAuth2RefreshCredentialsHandler` 从 Catalog 服务获取新的 OAuth2 Token。

### 5.5 Analytics Core 加速库集成

GCSFileIO 集成了 Google 的 Analytics Core 库来加速数据读取：

```java
// GCSInputFile.java (第79-92行)
@Override
public SeekableInputStream newStream() {
    if (gcpProperties().isGcsAnalyticsCoreEnabled()) {
        try {
            return newGoogleCloudStorageInputStream();
        } catch (IOException e) {
            LOG.error("Failed to create GCS analytics core input stream, falling back to default.", e);
        }
    }
    return new GCSInputStream(storage(), blobId(), blobSize, gcpProperties(), metrics());
}
```

当启用 `gcs.analytics-core.enabled` 时，GCSInputFile 会尝试使用 `GoogleCloudStorageInputStream` 来读取数据，这个类利用 `GcsFileSystem` 实现了高效的预取和缓存策略。如果创建失败，会自动回退到标准的 GCSInputStream。

### 5.6 配置参数完整清单

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `gcs.project-id` | GCP 项目 ID | - |
| `gcs.client-lib-token` | 客户端库 Token | - |
| `gcs.service.host` | GCS 服务主机 | - |
| `gcs.decryption-key` | 客户端解密密钥 | - |
| `gcs.encryption-key` | 客户端加密密钥 | - |
| `gcs.user-project` | 请求方付费的项目 ID | - |
| `gcs.channel.read.chunk-size-bytes` | 读取通道块大小 | GCS 默认 |
| `gcs.channel.write.chunk-size-bytes` | 写入通道块大小 | GCS 默认 |
| `gcs.oauth2.token` | OAuth2 访问令牌 | - |
| `gcs.oauth2.token-expires-at` | Token 过期时间戳 | - |
| `gcs.no-auth` | 禁用认证（测试用） | `false` |
| `gcs.oauth2.refresh-credentials-endpoint` | 凭证刷新端点 | - |
| `gcs.oauth2.refresh-credentials-enabled` | 启用凭证刷新 | `true` |
| `gcs.impersonate.service-account` | 模拟的服务账号 | - |
| `gcs.impersonate.lifetime-seconds` | 模拟凭证有效期 | `3600` |
| `gcs.impersonate.delegates` | 模拟委托链 | - |
| `gcs.impersonate.scopes` | 模拟范围 | `cloud-platform` |
| `gcs.delete.batch-size` | 批量删除大小 | `50` |
| `gcs.analytics-core.enabled` | 启用 Analytics Core 加速 | `false` |

---

## 6. ADLSFileIO：Azure ADLS Gen2 实现

### 6.1 整体架构

`ADLSFileIO`（`azure/src/main/java/org/apache/iceberg/azure/adlsv2/ADLSFileIO.java`）是基于 Azure Data Lake Storage Gen2 的 FileIO 实现。

```java
// ADLSFileIO.java (第50行)
public class ADLSFileIO implements DelegateFileIO {
```

ADLSFileIO 使用 Azure SDK 的 `DataLakeFileClient` 进行文件操作。与 S3FileIO 和 GCSFileIO 不同，ADLSFileIO 没有实现 `SupportsStorageCredentials` 接口，而是通过 `VendedAdlsCredentialProvider` 来处理凭证刷新。

### 6.2 ADLSLocation 路径解析

ADLSLocation 支持两种 URI 格式：

```java
// ADLSLocation.java (第47行)
private static final Pattern URI_PATTERN = Pattern.compile("^(abfss?|wasbs?)://([^/?#]+)(.*)?$");
```

支持的 URI 格式：
- `abfs://[container@]storageAccount.dfs.core.windows.net/path`
- `abfss://[container@]storageAccount.dfs.core.windows.net/path`（加密连接）
- `wasb://container@storageAccount.blob.core.windows.net/path`
- `wasbs://container@storageAccount.blob.core.windows.net/path`

解析逻辑：

```java
// ADLSLocation.java (第59-80行)
ADLSLocation(String location) {
    Matcher matcher = URI_PATTERN.matcher(location);
    ValidationException.check(matcher.matches(), "Invalid ADLS URI: %s", location);

    String authority = matcher.group(2);
    String[] parts = authority.split("@", -1);
    if (parts.length > 1) {
        this.container = parts[0];       // @前的部分是 container
        this.host = parts[1];             // @后的部分是 host
        this.storageAccount = host.split("\\.", -1)[0]; // host 的第一段是 storage account
    } else {
        this.container = null;
        this.host = authority;
        this.storageAccount = authority.split("\\.", -1)[0];
    }
    
    String uriPath = matcher.group(3);
    this.path = uriPath == null ? "" : uriPath.startsWith("/") ? uriPath.substring(1) : uriPath;
}
```

### 6.3 ADLSInputStream 范围读取

ADLSInputStream 使用 `DataLakeFileClient.openInputStream()` 实现流式读取：

```java
// ADLSInputStream.java (第85-89行)
private void openStream() {
    DataLakeFileOpenInputStreamResult result = openRange(new FileRange(pos));
    this.fileSize = result.getProperties().getFileSize();
    this.stream = result.getInputStream();
}
```

ADLSInputStream 的 seek 优化策略与 S3InputStream 相似，采用了 Read-through 模式：

```java
// ADLSInputStream.java (第139-163行)
private void positionStream() throws IOException {
    if ((stream != null) && (next == pos)) {
        return; // 已在正确位置
    }

    if ((stream != null) && (next > pos)) {
        long skip = next - pos;
        if (skip <= Math.max(stream.available(), SKIP_SIZE)) { // SKIP_SIZE = 1MB
            try {
                ByteStreams.skipFully(stream, skip);
                this.pos = next;
                return;
            } catch (IOException ignored) { }
        }
    }

    this.pos = next;
    openStream();
}
```

Range 读取通过 `DataLakeFileInputStreamOptions` 的 `FileRange` 参数实现：

```java
// ADLSInputStream.java (第166-174行)
@Override
public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    FileRange range = new FileRange(position, position + length);
    try (InputStream inputStream = openRange(range).getInputStream()) {
        IOUtil.readFully(inputStream, buffer, offset, length);
    }
}
```

ADLSInputStream 还有一个精心设计的异常处理机制，能够将 Azure 特有的异常（`BlobStorageException`、`DataLakeStorageException`）转换为 Iceberg 的 `NotFoundException`：

```java
// ADLSInputStream.java (第221-235行)
private static void throwNotFoundIfNotPresent(Throwable throwable, String location) {
    if (isFileNotFoundException(throwable)) {
        throw new NotFoundException(throwable, "Location does not exist: %s", location);
    }
}

private static boolean isFileNotFoundException(Throwable exception) {
    if (exception instanceof BlobStorageException blobStorageException) {
        return BlobErrorCode.BLOB_NOT_FOUND.equals(blobStorageException.getErrorCode());
    }
    if (exception instanceof DataLakeStorageException dataLakeStorageException) {
        return "PathNotFound".equals(dataLakeStorageException.getErrorCode());
    }
    return false;
}
```

### 6.4 ADLSOutputStream 输出流

ADLSOutputStream 的实现相对简洁，利用 Azure SDK 的 `DataLakeFileClient.getOutputStream()` 实现：

```java
// ADLSOutputStream.java (第93-103行)
private void openStream() {
    DataLakeFileOutputStreamOptions options = new DataLakeFileOutputStreamOptions();
    ParallelTransferOptions transferOptions = new ParallelTransferOptions();
    azureProperties.adlsWriteBlockSize().ifPresent(transferOptions::setBlockSizeLong);
    try {
        this.stream = new BufferedOutputStream(fileClient.getOutputStream(options));
    } catch (RuntimeException e) {
        LOG.error("Failed to open output stream for file {}", fileClient.getFilePath(), e);
        throw e;
    }
}
```

Azure SDK 底层已经实现了分块上传逻辑（Block Blob），块大小可以通过 `adls.write.block-size-bytes` 配置。ADLSOutputStream 额外包装了 `BufferedOutputStream` 来减少写入调用次数。

### 6.5 多种认证方式

ADLSFileIO 通过 `AzureProperties.applyClientConfiguration()` 支持四种认证方式：

```java
// AzureProperties.java (第171-205行)
public void applyClientConfiguration(String account, DataLakeFileSystemClientBuilder builder) {
    if (!adlsRefreshCredentialsEnabled || Strings.isNullOrEmpty(adlsRefreshCredentialsEndpoint)) {
        // 1. SAS Token 认证
        String sasToken = adlsSasTokens.get(account);
        if (sasToken != null && !sasToken.isEmpty()) {
            builder.sasToken(sasToken);
        }
        // 2. SharedKey 认证
        else if (namedKeyCreds != null) {
            builder.credential(new StorageSharedKeyCredential(namedKeyCreds.getKey(), namedKeyCreds.getValue()));
        }
        // 3. OAuth2 Token 认证
        else if (token != null && !token.isEmpty()) {
            TokenCredential tokenCredential = new TokenCredential() {
                @Override
                public Mono<AccessToken> getToken(TokenRequestContext request) {
                    return Mono.just(new AccessToken(token, OffsetDateTime.now(ZoneOffset.UTC).plusHours(1)));
                }
            };
            builder.credential(tokenCredential);
        }
        // 4. 自定义 TokenCredentialProvider（DefaultAzureCredential 等）
        else {
            AdlsTokenCredentialProvider credentialProvider = AdlsTokenCredentialProviders.from(allProperties);
            builder.credential(credentialProvider.credential());
        }
    }
}
```

认证优先级从高到低：
1. **SAS Token**（`adls.sas-token.{account}`）：短期凭证，适合跨组织共享
2. **SharedKey**（`adls.auth.shared-key.account.name/key`）：对称密钥认证
3. **OAuth2 Token**（`adls.token`）：从 REST Catalog 获取的 Bearer Token
4. **自定义 Provider**（`adls.token-credential-provider`）：用户自定义的 TokenCredentialProvider 实现

### 6.6 Vended Credentials 支持

当配置了 `adls.refresh-credentials-endpoint` 时，ADLSFileIO 使用 `VendedAdlsCredentialProvider` 来处理凭证：

```java
// ADLSFileIO.java (第167-174行)
@Override
public void initialize(Map<String, String> props) {
    this.properties = SerializableMap.copyOf(props);
    this.azureProperties = new AzureProperties(properties);
    initMetrics(properties);
    this.azureProperties
        .vendedAdlsCredentialProvider()
        .ifPresent(provider -> this.vendedAdlsCredentialProvider = provider);
}
```

`VendedAdlsCredentialProvider` 作为 Azure HTTP Pipeline 的 Policy 注入到客户端中，通过 `VendedAzureSasCredentialPolicy` 自动为请求添加 SAS Token。

### 6.7 配置参数完整清单

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `adls.sas-token.{account}` | 指定存储账户的 SAS Token | - |
| `adls.sas-token-expires-at-ms.{account}` | SAS Token 过期时间 | - |
| `adls.connection-string.{account}` | 连接字符串（可覆盖端点） | - |
| `adls.read.block-size-bytes` | 读取块大小 | Azure SDK 默认 |
| `adls.write.block-size-bytes` | 写入块大小 | Azure SDK 默认 |
| `adls.auth.shared-key.account.name` | SharedKey 账户名 | - |
| `adls.auth.shared-key.account.key` | SharedKey 密钥 | - |
| `adls.token` | OAuth2 Bearer Token | - |
| `adls.token-credential-provider` | 自定义 TokenCredentialProvider 类 | - |
| `adls.token-credential-provider.*` | Provider 特定属性 | - |
| `adls.refresh-credentials-endpoint` | 凭证刷新端点 | - |
| `adls.refresh-credentials-enabled` | 启用凭证刷新 | `true` |
| `azure.keyvault.url` | Azure Key Vault URL | - |
| `azure.keyvault.key-wrap-algorithm` | Key Vault 密钥包装算法 | `RSA-OAEP-256` |

---

## 7. ResolvingFileIO：动态路由实现

### 7.1 设计动机

在实际部署中，一个 Iceberg 表的数据可能分布在不同的存储系统上。例如，元数据文件可能存储在 HDFS 上，而数据文件存储在 S3 上。ResolvingFileIO 正是为这种混合存储场景设计的。

### 7.2 URI Scheme 路由机制

ResolvingFileIO 通过 URI scheme 自动选择底层 FileIO 实现：

```java
// ResolvingFileIO.java (第57-69行)
private static final String FALLBACK_IMPL = "org.apache.iceberg.hadoop.HadoopFileIO";
private static final String S3_FILE_IO_IMPL = "org.apache.iceberg.aws.s3.S3FileIO";
private static final String GCS_FILE_IO_IMPL = "org.apache.iceberg.gcp.gcs.GCSFileIO";
private static final String ADLS_FILE_IO_IMPL = "org.apache.iceberg.azure.adlsv2.ADLSFileIO";

private static final Map<String, String> SCHEME_TO_FILE_IO =
    ImmutableMap.of(
        "s3",    S3_FILE_IO_IMPL,
        "s3a",   S3_FILE_IO_IMPL,
        "s3n",   S3_FILE_IO_IMPL,
        "gs",    GCS_FILE_IO_IMPL,
        "abfs",  ADLS_FILE_IO_IMPL,
        "abfss", ADLS_FILE_IO_IMPL,
        "wasb",  ADLS_FILE_IO_IMPL,
        "wasbs", ADLS_FILE_IO_IMPL);
```

路由逻辑简洁明了：

```java
// ResolvingFileIO.java (第234-255行)
String implFromLocation(String location) {
    return SCHEME_TO_FILE_IO.getOrDefault(scheme(location), FALLBACK_IMPL);
}

private static String scheme(String location) {
    int colonPos = location.indexOf(":");
    if (colonPos > 0) {
        return location.substring(0, colonPos);
    }
    return null;
}
```

通过截取 URI 中第一个冒号之前的部分作为 scheme，然后查表决定使用哪个 FileIO 实现。如果 scheme 无法识别（如 `hdfs://`、`file://`），则回退到 HadoopFileIO。

### 7.3 实例缓存与生命周期管理

ResolvingFileIO 对委托的 FileIO 实例进行缓存，避免重复创建：

```java
// ResolvingFileIO.java (第71-72行)
private final Map<String, DelegateFileIO> ioInstances = Maps.newConcurrentMap();
private final AtomicBoolean isClosed = new AtomicBoolean(false);
```

`io(location)` 方法实现了带缓存的实例创建：

```java
// ResolvingFileIO.java (第166-232行)
DelegateFileIO io(String location) {
    String impl = implFromLocation(location);
    DelegateFileIO io = ioInstances.get(impl);
    if (io != null) {
        // 处理 Kryo 反序列化后 Hadoop Config 为 null 的情况
        if (io instanceof HadoopConfigurable && ((HadoopConfigurable) io).getConf() == null) {
            synchronized (io) {
                if (((HadoopConfigurable) io).getConf() == null) {
                    ((HadoopConfigurable) io).setConf(getConf());
                }
            }
        }
        // 同步 StorageCredentials
        if (io instanceof SupportsStorageCredentials
            && !((SupportsStorageCredentials) io).credentials().equals(storageCredentials)) {
            ((SupportsStorageCredentials) io).setCredentials(storageCredentials);
        }
        return io;
    }

    return ioInstances.computeIfAbsent(impl, key -> {
        FileIO fileIO;
        try {
            Map<String, String> props = Maps.newHashMap(properties);
            props.put("init-creation-stacktrace", "false"); // ResolvingFileIO 自己追踪
            fileIO = CatalogUtil.loadFileIO(key, props, conf, storageCredentials);
        } catch (IllegalArgumentException e) {
            if (key.equals(FALLBACK_IMPL)) {
                throw e;
            } else {
                // 加载失败，回退到 HadoopFileIO
                LOG.warn("Failed to load FileIO: {}, falling back to {}", key, FALLBACK_IMPL, e);
                fileIO = CatalogUtil.loadFileIO(FALLBACK_IMPL, properties, conf, storageCredentials);
            }
        }
        
        Preconditions.checkState(fileIO instanceof DelegateFileIO, ...);
        return (DelegateFileIO) fileIO;
    });
}
```

**设计亮点**：

1. 使用 `ConcurrentHashMap.computeIfAbsent` 确保每种 FileIO 实现只创建一个实例
2. Kryo 反序列化后，Hadoop Configuration 可能为 null，需要在首次使用时重新注入
3. StorageCredentials 变化时自动同步到委托 FileIO
4. 二级回退机制：先尝试目标实现，失败后回退到 HadoopFileIO

### 7.4 Fallback 机制

ResolvingFileIO 的 fallback 是两级的：

1. **一级 Fallback**：如果目标 FileIO 类（如 S3FileIO）无法加载（比如缺少依赖 jar），自动回退到 HadoopFileIO
2. **二级 Fallback**：如果 HadoopFileIO 也无法加载，则抛出原始异常

这个机制使得 ResolvingFileIO 在运行时环境缺少某些依赖时仍能正常工作（只要 Hadoop 可用）。

### 7.5 Storage Credentials 传递

ResolvingFileIO 作为中间层，负责将 StorageCredentials 传递到实际的 FileIO 实现：

```java
// ResolvingFileIO.java (第283-292行)
@Override
public void setCredentials(List<StorageCredential> credentials) {
    Preconditions.checkArgument(credentials != null, "Invalid storage credentials: null");
    this.storageCredentials = Lists.newArrayList(credentials);
}
```

在 `io(location)` 方法中，每次返回已缓存的 FileIO 实例时都会检查凭证是否需要更新。这确保了凭证刷新可以自动传播到底层实现。

### 7.6 资源泄露检测

ResolvingFileIO 实现了 `finalize()` 方法来检测未正确关闭的实例：

```java
// ResolvingFileIO.java (第257-270行)
@Override
protected void finalize() throws Throwable {
    super.finalize();
    if (!isClosed.get()) {
        close();
        if (null != createStack) {
            String trace = Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
            LOG.warn("Unclosed ResolvingFileIO instance created by:\n\t{}", trace);
        }
    }
}
```

在构造函数中捕获创建时的堆栈轨迹，如果 GC 回收时发现未关闭，会打印警告日志和创建位置。这个模式在 S3FileIO、S3InputStream、S3OutputStream 中也被使用。

---

## 8. EncryptingFileIO：加密层封装

### 8.1 信封加密模式

Iceberg 的加密采用信封加密（Envelope Encryption）模式，`EncryptingFileIO` 是加密功能的门面类。

信封加密的核心思想：
1. 使用 **数据加密密钥（DEK）** 加密实际数据
2. 使用 **密钥加密密钥（KEK）** 加密 DEK
3. 加密后的 DEK（key metadata）存储在 Iceberg 的元数据中

### 8.2 EncryptionManager 接口

`EncryptingFileIO` 持有一个 `EncryptionManager` 实例，负责实际的加解密操作：

```java
// EncryptingFileIO.java (第40-64行)
public class EncryptingFileIO implements FileIO, Serializable {
    public static EncryptingFileIO combine(FileIO io, EncryptionManager em) {
        if (io instanceof EncryptingFileIO) {
            EncryptingFileIO encryptingIO = (EncryptingFileIO) io;
            if (encryptingIO.em == em) return encryptingIO;
            return combine(encryptingIO.io, em);
        }
        if (io instanceof SupportsPrefixOperations) {
            return new WithSupportsPrefixOperations((SupportsPrefixOperations) io, em);
        } else {
            return new EncryptingFileIO(io, em);
        }
    }

    private final FileIO io;
    private final EncryptionManager em;
}
```

`combine()` 静态工厂方法的设计体现了几个巧妙之处：

1. **幂等组合**：如果已经是 EncryptingFileIO 且使用相同的 EncryptionManager，直接返回
2. **避免嵌套**：如果已经是 EncryptingFileIO，则剥离外层后与新的 EncryptionManager 重新组合
3. **保留能力**：如果底层 IO 支持 `SupportsPrefixOperations`，使用专门的子类保留此能力

### 8.3 透明加解密流程

**读取（解密）流程**：

```java
// EncryptingFileIO.java (第92-107行)
@Override
public InputFile newInputFile(DataFile file) {
    return newInputFile((ContentFile<?>) file);
}

private InputFile newInputFile(ContentFile<?> file) {
    if (file.keyMetadata() != null) {
        return newDecryptingInputFile(file.location(), file.fileSizeInBytes(), file.keyMetadata());
    } else {
        return newInputFile(file.location(), file.fileSizeInBytes());
    }
}

public InputFile newDecryptingInputFile(String path, long length, ByteBuffer buffer) {
    return em.decrypt(wrap(io.newInputFile(path, length), buffer));
}
```

当文件带有 `keyMetadata` 时，EncryptingFileIO 将加密的 InputFile 和密钥元数据包装为 `EncryptedInputFile`，然后交给 EncryptionManager 解密。

**写入（加密）流程**：

```java
// EncryptingFileIO.java (第137-144行)
@Override
public OutputFile newOutputFile(String path) {
    return io.newOutputFile(path);
}

public EncryptedOutputFile newEncryptingOutputFile(String path) {
    OutputFile plainOutputFile = io.newOutputFile(path);
    return em.encrypt(plainOutputFile);
}
```

注意 `newOutputFile` 返回的是普通的未加密 OutputFile，而 `newEncryptingOutputFile` 返回的是 `EncryptedOutputFile`，后者包含了加密所需的密钥信息。

**批量解密**：

```java
// EncryptingFileIO.java (第66-75行)
public Map<String, InputFile> bulkDecrypt(Iterable<? extends ContentFile<?>> files) {
    Iterable<InputFile> decrypted = em.decrypt(Iterables.transform(files, this::wrap));
    ImmutableMap.Builder<String, InputFile> builder = ImmutableMap.builder();
    for (InputFile in : decrypted) {
        builder.put(in.location(), in);
    }
    return builder.buildKeepingLast();
}
```

批量解密允许 EncryptionManager 进行优化，比如对使用相同 KEK 加密的文件只解密一次 KEK。

### 8.4 与 SupportsPrefixOperations 的兼容

```java
// EncryptingFileIO.java (第221-240行)
static class WithSupportsPrefixOperations extends EncryptingFileIO
    implements SupportsPrefixOperations {
    private final SupportsPrefixOperations prefixIo;

    WithSupportsPrefixOperations(SupportsPrefixOperations io, EncryptionManager em) {
        super(io, em);
        this.prefixIo = io;
    }

    @Override
    public Iterable<FileInfo> listPrefix(String prefix) {
        return prefixIo.listPrefix(prefix);
    }

    @Override
    public void deletePrefix(String prefix) {
        prefixIo.deletePrefix(prefix);
    }
}
```

这个子类确保了当底层 FileIO 支持 `SupportsPrefixOperations` 时，EncryptingFileIO 也继承此能力。`listPrefix` 和 `deletePrefix` 不需要加解密逻辑，直接委托。

---

## 9. FileIO 的序列化机制

### 9.1 SerializableTable 中的 FileIO

在分布式计算引擎中，表对象需要被序列化发送到 Worker 节点。`SerializableTable` 是 Iceberg 的可序列化表实现：

```java
// SerializableTable.java (第50-96行)
public class SerializableTable implements Table, HasTableOperations, Serializable {
    private final FileIO io;
    private final EncryptionManager encryption;
    // ...
    
    protected SerializableTable(Table table) {
        // ...
        this.io = table.io();
        this.encryption = table.encryption();
        // ...
    }
}
```

`SerializableTable` 直接持有 FileIO 引用。由于 FileIO 接口继承了 `Serializable`，Java 序列化框架会自动序列化 FileIO 及其所有可序列化的字段。

但这里面有一个关键挑战：**Hadoop Configuration 不可序列化**。

### 9.2 Hadoop Configuration 序列化

Hadoop 的 `Configuration` 类虽然实现了 `Writable` 接口，但不是 `Serializable`。Iceberg 通过 `SerializableConfiguration` 解决了这个问题：

```java
// SerializableConfiguration.java (第28-51行)
public class SerializableConfiguration implements SerializableSupplier<Configuration> {
    private final Map<String, String> confAsMap;
    private transient volatile Configuration hadoopConf = null;

    public SerializableConfiguration(Configuration hadoopConf) {
        this.confAsMap = Maps.newHashMapWithExpectedSize(hadoopConf.size());
        hadoopConf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public Configuration get() {
        if (hadoopConf == null) {
            synchronized (this) {
                if (hadoopConf == null) {
                    Configuration newConf = new Configuration(false);
                    confAsMap.forEach(newConf::set);
                    this.hadoopConf = newConf;
                }
            }
        }
        return hadoopConf;
    }
}
```

核心策略：
1. 序列化时将 Configuration 展平为 `Map<String, String>`
2. 反序列化后从 Map 重建 Configuration
3. 使用 `new Configuration(false)` 跳过默认配置加载
4. Configuration 对象声明为 `transient volatile`，避免序列化并确保多线程可见性

### 9.3 HadoopConfigurable 接口

`HadoopConfigurable` 接口提供了自定义 Configuration 序列化策略的能力：

```java
// HadoopConfigurable.java (第34-44行)
public interface HadoopConfigurable extends Configurable {
    void serializeConfWith(
        Function<Configuration, SerializableSupplier<Configuration>> confSerializer);
}
```

这个接口允许外部代码（如 Spark 集成层）注入自定义的 Configuration 序列化器。例如，Spark 可能使用自己的 `SerializableWritable` 来序列化 Configuration，而不是使用 Iceberg 默认的 `SerializableConfiguration`。

HadoopFileIO 和 ResolvingFileIO 都实现了此接口：

```java
// HadoopFileIO.java (第145-149行)
@Override
public void serializeConfWith(
    Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    this.hadoopConf = confSerializer.apply(getConf());
}
```

### 9.4 FileIOParser JSON 序列化

除了 Java 原生序列化，Iceberg 还支持通过 JSON 序列化/反序列化 FileIO：

```java
// FileIOParser.java (第29-81行)
public class FileIOParser {
    private static final String FILE_IO_IMPL = "io-impl";
    private static final String PROPERTIES = "properties";

    public static String toJson(FileIO io) {
        String impl = io.getClass().getName();
        Map<String, String> properties = io.properties();
        // 写入 JSON: {"io-impl": "...", "properties": {...}}
    }

    public static FileIO fromJson(String json, Object conf) {
        String impl = JsonUtil.getString(FILE_IO_IMPL, json);
        Map<String, String> properties = JsonUtil.getStringMap(PROPERTIES, json);
        return CatalogUtil.loadFileIO(impl, properties, conf);
    }
}
```

JSON 序列化只保存 FileIO 的类名和配置属性，反序列化时通过动态类加载重建实例。这种方式更加紧凑，且对序列化框架没有要求。

### 9.5 Kryo 序列化兼容性

Iceberg 在多处为 Kryo 序列化做了特殊处理。Kryo 是 Spark 推荐的序列化框架，但与 Java 默认序列化有一些差异：

1. **可变集合**：ResolvingFileIO 和 S3FileIO 中的 `storageCredentials` 使用 `Lists.newArrayList()` 而非 ImmutableList，因为 Kryo 对不可变集合的支持有限

```java
// ResolvingFileIO.java (第77行)
private List<StorageCredential> storageCredentials = Lists.newArrayList();
```

2. **transient volatile 字段**：所有不可序列化的字段（如客户端连接、线程池等）都声明为 `transient volatile`，Kryo 序列化后需要在使用时重新初始化

```java
// S3FileIO.java (第115行)
private transient volatile Map<String, PrefixedS3Client> clientByPrefix;
```

3. **Hadoop Configuration 恢复**：ResolvingFileIO 在返回缓存的 FileIO 实例时，会检查并重新注入 Hadoop Configuration

```java
// ResolvingFileIO.java (第170-176行)
if (io instanceof HadoopConfigurable && ((HadoopConfigurable) io).getConf() == null) {
    synchronized (io) {
        if (((HadoopConfigurable) io).getConf() == null) {
            ((HadoopConfigurable) io).setConf(getConf());
        }
    }
}
```

---

## 10. 凭证管理与刷新机制

### 10.1 StorageCredential 体系

`StorageCredential` 是 Iceberg 的存储凭证抽象：

```java
// StorageCredential.java (第25-39行)
public interface StorageCredential extends Serializable {
    String prefix();              // 存储路径前缀
    Map<String, String> config(); // 凭证配置键值对
    
    default void validate() {
        Preconditions.checkArgument(!prefix().isEmpty(), "Invalid prefix: must be non-empty");
        Preconditions.checkArgument(!config().isEmpty(), "Invalid config: must be non-empty");
    }

    static StorageCredential create(String prefix, Map<String, String> config) {
        return ImmutableStorageCredential.builder().prefix(prefix).config(config).build();
    }
}
```

StorageCredential 的设计基于**前缀匹配**模型：每个凭证关联一个存储路径前缀，访问该前缀下的文件时使用对应的凭证。这支持了以下场景：

- 同一个 FileIO 实例访问多个具有不同凭证的存储位置
- 不同 bucket 使用不同的 IAM 角色
- 跨账户存储访问

### 10.2 SupportsStorageCredentials 接口

```java
// SupportsStorageCredentials.java (第27-32行)
public interface SupportsStorageCredentials {
    void setCredentials(List<StorageCredential> credentials);
    List<StorageCredential> credentials();
}
```

这个接口是 REST Catalog 凭证注入的关键。REST Catalog 服务可以通过 `/v1/credentials` 端点下发存储凭证，客户端通过此接口将凭证注入到 FileIO 中。

### 10.3 VendedCredentialsProvider (S3)

S3 的凭证刷新通过 `VendedCredentialsProvider` 实现，它实现了 AWS SDK 的 `AwsCredentialsProvider` 接口：

```java
// VendedCredentialsProvider.java (第48-196行)
public class VendedCredentialsProvider implements AwsCredentialsProvider, SdkAutoCloseable {
    private final CachedSupplier<AwsCredentials> credentialCache;

    @Override
    public AwsCredentials resolveCredentials() {
        return credentialCache.get();
    }
}
```

凭证获取流程：
1. 首先检查 properties 中是否已有有效凭证（`credentialFromProperties()`）
2. 如果凭证即将过期（5 分钟内），通过 REST API 刷新
3. 使用 AWS SDK 的 `CachedSupplier` 管理缓存和预刷新

```java
// VendedCredentialsProvider.java (第153-190行)
private RefreshResult<AwsCredentials> refreshCredential() {
    LoadCredentialsResponse response = fetchCredentials();
    // 解析响应中的 S3 凭证
    Credential s3Credential = s3Credentials.get(0);
    
    String accessKeyId = s3Credential.config().get(S3FileIOProperties.ACCESS_KEY_ID);
    String secretAccessKey = s3Credential.config().get(S3FileIOProperties.SECRET_ACCESS_KEY);
    String sessionToken = s3Credential.config().get(S3FileIOProperties.SESSION_TOKEN);
    String tokenExpiresAtMillis = s3Credential.config().get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS);
    
    Instant expiresAt = Instant.ofEpochMilli(Long.parseLong(tokenExpiresAtMillis));
    Instant prefetchAt = expiresAt.minus(5, ChronoUnit.MINUTES);

    return RefreshResult.builder(AwsSessionCredentials.builder()
            .accessKeyId(accessKeyId)
            .secretAccessKey(secretAccessKey)
            .sessionToken(sessionToken)
            .expirationTime(expiresAt)
            .build())
        .staleTime(expiresAt)
        .prefetchTime(prefetchAt)
        .build();
}
```

### 10.4 OAuth2RefreshCredentialsHandler (GCS)

GCSFileIO 使用 `OAuth2RefreshCredentialsHandler` 刷新 OAuth2 Token：

```java
// GCSFileIO.java (第240-259行)
private void refreshStorageCredentials() {
    if (isResourceClosed.get()) return;

    try (OAuth2RefreshCredentialsHandler handler =
        OAuth2RefreshCredentialsHandler.create(properties)) {
        List<StorageCredential> refreshed =
            handler.fetchCredentials().credentials().stream()
                .filter(c -> c.prefix().startsWith(ROOT_STORAGE_PREFIX))
                .map(c -> StorageCredential.create(c.prefix(), c.config()))
                .toList();

        if (!refreshed.isEmpty() && !isResourceClosed.get()) {
            this.storageCredentials = Lists.newArrayList(refreshed);
            scheduleCredentialRefresh();
        }
    } catch (Exception e) {
        LOG.warn("Failed to refresh storage credentials", e);
    }
}
```

刷新机制与 S3 类似：
1. 计算最早过期时间
2. 提前 5 分钟调度刷新
3. 通过 REST API 获取新凭证
4. 更新本地凭证并重建存储客户端

### 10.5 VendedAdlsCredentialProvider (Azure)

Azure 的凭证刷新通过 `VendedAdlsCredentialProvider` 实现，它作为 Azure HTTP Pipeline 的 Policy 注入到客户端中。当配置了 `adls.refresh-credentials-endpoint` 时自动启用。

这种设计的优势在于，凭证刷新是透明的 -- 每个请求经过 Pipeline 时，Policy 会自动检查并刷新 SAS Token，不需要重建客户端。

---

## 11. ContentCache 文件内容缓存

### 11.1 缓存设计

`ContentCache`（`core/src/main/java/org/apache/iceberg/io/ContentCache.java`）基于 Caffeine 缓存库实现了文件内容级的缓存，主要用于缓存频繁读取的小文件（如 manifest 文件、metadata 文件）。

```java
// ContentCache.java (第51-96行)
public class ContentCache {
    private static final int BUFFER_CHUNK_SIZE = 4 * 1024 * 1024; // 4MB
    
    private final long expireAfterAccessMs;
    private final long maxTotalBytes;
    private final long maxContentLength;
    private final Cache<String, FileContent> cache;

    public ContentCache(long expireAfterAccessMs, long maxTotalBytes, long maxContentLength) {
        Caffeine<Object, Object> builder = Caffeine.newBuilder();
        if (expireAfterAccessMs > 0) {
            builder = builder.expireAfterAccess(Duration.ofMillis(expireAfterAccessMs));
        }
        this.cache = builder
            .maximumWeight(maxTotalBytes)
            .weigher((Weigher<String, FileContent>) (key, value) ->
                (int) Math.min(value.length, Integer.MAX_VALUE))
            .softValues()      // 使用 SoftReference，内存压力时允许 GC 回收
            .removalListener((location, fileContent, cause) ->
                LOG.debug("Evicted {} from ContentCache ({})", location, cause))
            .recordStats()     // 启用统计
            .build();
    }
}
```

缓存策略：
- **容量限制**：通过 `maximumWeight` 限制总缓存字节数
- **过期策略**：访问后过期（access-based expiration）
- **软引用**：使用 `softValues()` 确保内存压力时不会导致 OOM
- **文件大小过滤**：超过 `maxContentLength` 的文件不会被缓存

### 11.2 CachingInputFile 实现

```java
// ContentCache.java (第190-250行)
private static class CachingInputFile implements InputFile {
    private final ContentCache contentCache;
    private final InputFile input;

    @Override
    public SeekableInputStream newStream() {
        try {
            return cachedStream();
        } catch (FileNotFoundException e) {
            throw new NotFoundException(e, "Failed to open file: %s", input.location());
        } catch (IOException e) {
            return input.newStream(); // 缓存失败，回退到直接读取
        }
    }

    private SeekableInputStream cachedStream() throws IOException {
        try {
            FileContent content = contentCache.cache.get(input.location(), k -> download(input));
            return ByteBufferInputStream.wrap(content.buffers);
        } catch (UncheckedIOException ex) {
            throw ex.getCause();
        }
    }
}
```

CachingInputFile 的巧妙之处在于：
1. 首次调用 `newStream()` 时会下载整个文件到缓存
2. 后续调用直接从缓存的 ByteBuffer 创建 ByteBufferInputStream
3. 如果缓存加载失败，自动回退到直接读取底层 InputFile

文件内容以 4MB 的 ByteBuffer 块列表形式存储，这避免了单个超大 ByteBuffer 的内存分配压力。

---

## 12. FileIO 加载机制：CatalogUtil.loadFileIO()

### 12.1 动态加载流程

`CatalogUtil.loadFileIO()` 是 FileIO 的动态加载入口：

```java
// CatalogUtil.java (第391-425行)
@SuppressWarnings("unchecked")
public static FileIO loadFileIO(
    String impl,
    Map<String, String> properties,
    Object hadoopConf,
    List<StorageCredential> storageCredentials) {
    
    LOG.info("Loading custom FileIO implementation: {}", impl);
    
    // 1. 通过反射创建实例
    DynConstructors.Ctor<FileIO> ctor;
    try {
        ctor = DynConstructors.builder(FileIO.class)
            .loader(CatalogUtil.class.getClassLoader())
            .impl(impl)
            .buildChecked();
    } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
            String.format("Cannot initialize FileIO implementation %s: %s", impl, e.getMessage()), e);
    }

    FileIO fileIO;
    try {
        fileIO = ctor.newInstance();
    } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            String.format("Cannot initialize FileIO, %s does not implement FileIO.", impl), e);
    }

    // 2. 注入 Hadoop 配置
    configureHadoopConf(fileIO, hadoopConf);
    
    // 3. 注入 Storage Credentials
    if (fileIO instanceof SupportsStorageCredentials) {
        ((SupportsStorageCredentials) fileIO).setCredentials(storageCredentials);
    }

    // 4. 初始化
    fileIO.initialize(properties);
    return fileIO;
}
```

加载流程严格按照以下顺序执行：
1. **反射创建**：使用无参构造函数创建 FileIO 实例
2. **Hadoop 配置注入**：如果 FileIO 是 HadoopConfigurable（或 Hadoop 的 Configurable），注入 Configuration
3. **凭证注入**：如果 FileIO 支持 SupportsStorageCredentials，注入存储凭证
4. **初始化**：调用 `initialize(properties)` 传递配置属性

**顺序很重要**：配置注入在 `initialize()` 之前，确保 `initialize()` 中可以使用已注入的配置和凭证。

### 12.2 Hadoop 配置注入

`configureHadoopConf()` 方法支持两种方式注入 Hadoop 配置：

```java
// CatalogUtil.java (第434-469行)
public static void configureHadoopConf(Object maybeConfigurable, Object conf) {
    if (conf == null) return;

    // 1. 优先使用 Iceberg 的 Configurable 接口
    if (maybeConfigurable instanceof Configurable) {
        ((Configurable<Object>) maybeConfigurable).setConf(conf);
        return;
    }

    // 2. 尝试使用 Hadoop 的 Configurable 接口（通过反射）
    ClassLoader maybeConfigurableLoader = maybeConfigurable.getClass().getClassLoader();
    Class<?> configurableInterface;
    try {
        configurableInterface = DynClasses.builder()
            .loader(maybeConfigurableLoader)
            .impl("org.apache.hadoop.conf.Configurable")
            .buildChecked();
    } catch (ClassNotFoundException e) {
        return; // Hadoop 不在 classpath 中
    }

    if (!configurableInterface.isInstance(maybeConfigurable)) {
        return; // 不实现 Configurable
    }
    // ... 通过反射调用 setConf
}
```

这个设计使得即使在 Hadoop 不在 classpath 中的环境（比如纯 S3 部署）中，也能正常工作。

### 12.3 Storage Credentials 注入

Storage Credentials 的注入是条件性的：

```java
// CatalogUtil.java (第419-421行)
if (fileIO instanceof SupportsStorageCredentials) {
    ((SupportsStorageCredentials) fileIO).setCredentials(storageCredentials);
}
```

只有实现了 `SupportsStorageCredentials` 接口的 FileIO 才会接收凭证注入。这允许简单的 FileIO 实现（如 HadoopFileIO）不需要关心凭证管理。

---

## 13. 性能优化与调优指南

### 13.1 连接池与并发配置

**S3FileIO 线程池**

S3FileIO 使用两个线程池：
1. **上传线程池**：`S3OutputStream` 中的静态线程池，用于多段上传的并行分段传输。默认线程数为 CPU 核心数（`s3.multipart.num-threads`）。
2. **任务线程池**：`S3FileIO` 中的 `ScheduledExecutorService`，用于批量删除和凭证刷新。默认线程数取自 `s3.delete.num-threads`。

```java
// S3OutputStream.java (第111-125行)
if (executorService == null) {
    synchronized (S3OutputStream.class) {
        if (executorService == null) {
            executorService = MoreExecutors.getExitingExecutorService(
                (ThreadPoolExecutor) Executors.newFixedThreadPool(
                    s3FileIOProperties.multipartUploadThreads(),
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("iceberg-s3fileio-upload-%d")
                        .build()));
        }
    }
}
```

所有线程池都使用 daemon 线程并且是 `ExitingExecutorService`/`ExitingScheduledPool`，确保 JVM 退出时不会被阻塞。

**HadoopFileIO 线程池**

```java
// HadoopFileIO.java (第205-221行)
private int deleteThreads() {
    int defaultValue = Runtime.getRuntime().availableProcessors() * DEFAULT_DELETE_CORE_MULTIPLE;
    return conf().getInt(DELETE_FILE_PARALLELISM, defaultValue);
}
```

默认为 `CPU核心数 * 4`，可通过 `iceberg.hadoop.delete-file-parallelism` 配置。

**调优建议**：

- 对于高吞吐场景，增加 `s3.multipart.num-threads` 到 CPU 核心数的 2-4 倍
- 对于大规模表维护（expire snapshots），增加 `s3.delete.num-threads`
- 注意线程池是全局共享的（静态变量），同一 JVM 中的所有 FileIO 实例共用

### 13.2 预取与缓冲区优化

**ContentCache 配置**

ContentCache 主要用于缓存元数据文件（manifest、metadata JSON）。关键参数通常通过 Table Properties 配置：

```
io.manifest.cache-enabled = true
io.manifest.cache.expiration-interval-ms = 300000  (5分钟)
io.manifest.cache.max-total-bytes = 104857600      (100MB)
io.manifest.cache.max-content-length = 8388608     (8MB)
```

调优建议：
- 启用 ContentCache 可以显著减少对远程存储的请求次数
- 对于频繁扫描的场景，增大 `max-total-bytes`
- 对于 manifest 文件特别大的表，增大 `max-content-length`

**S3 Read-through Skip**

S3InputStream 的 `skipSize`（默认 1MB）控制了 seek 操作的行为：

```java
// S3InputStream.java (第71行)
private int skipSize = 1024 * 1024;
```

- 小于 `skipSize` 的前跳通过读取并丢弃数据实现，避免关闭/重建 HTTP 连接
- 大于 `skipSize` 的跳转会关闭当前流并从新位置打开
- 对于列式文件格式的顺序扫描，这个默认值通常足够

**GCS Channel 块大小**

```
gcs.channel.read.chunk-size-bytes  -- 控制 ReadChannel 的预取块大小
gcs.channel.write.chunk-size-bytes -- 控制 WriteChannel 的上传块大小
```

增大读取块大小可以减少 HTTP 请求次数，但会增加内存使用。

### 13.3 S3 专项调优

**多段上传分段大小**

```
s3.multipart.part-size-bytes = 33554432  (32MB，默认)
```

- 最小值：5MB（S3 限制）
- 最大值：需要小于 2GB
- S3 最多 10000 个分段，因此单文件最大 = part-size * 10000
- 对于写入大文件的场景，可以增大分段大小

**多段上传阈值**

```
s3.multipart.threshold = 1.5  (默认)
```

当文件大小超过 `part-size * threshold` 时切换到多段上传。对于主要写小文件的场景，可以适当增大此值以避免不必要的多段上传开销。

**重试策略**

```
s3.retry.num-retries = 5
s3.retry.min-wait-ms = 2000
s3.retry.max-wait-ms = 20000
```

这控制的是 AWS SDK 级别的重试，使用 Equal Jitter Backoff 策略。S3InputStream 还有自己的 Failsafe 重试（最多 3 次）。

**删除批量大小**

```
s3.delete.batch-size = 250  (默认)
```

S3 `DeleteObjects` API 最多支持 1000 个对象，但默认使用 250 是基于 Apache Hadoop 社区的经验值，在可靠性和效率之间取得平衡。

**传输加速**

```
s3.acceleration-enabled = true
```

启用 S3 Transfer Acceleration，对跨区域数据传输有显著加速效果。

**S3 Analytics Accelerator**

```
s3.analytics-accelerator.enabled = true
s3.crt.enabled = true
s3.crt.max-concurrency = 500
```

S3 Analytics Accelerator 是 AWS 开源的客户端加速库，通过智能预取和缓存优化大规模分析读取性能。需要配合 S3 CRT（C Runtime）异步客户端使用。

### 13.4 GCS 专项调优

**读写通道块大小**

```
gcs.channel.read.chunk-size-bytes = 2097152   (建议 2MB)
gcs.channel.write.chunk-size-bytes = 16777216  (建议 16MB)
```

**批量删除大小**

```
gcs.delete.batch-size = 50  (默认)
```

GCS 建议的最大批量操作数为 100，默认 50 留有余量。

**Analytics Core**

```
gcs.analytics-core.enabled = true
```

启用 Google 的 Analytics Core 库，提供优化的 GCS 读取性能。

### 13.5 ADLS 专项调优

**读写块大小**

```
adls.read.block-size-bytes  -- ADLS 读取请求的块大小
adls.write.block-size-bytes -- Block Blob 的块大小
```

- 增大写入块大小可以减少分块数量，提高大文件写入效率
- 增大读取块大小可以减少 HTTP 请求次数

**连接复用**

ADLSFileIO 在内部缓存了 `DataLakeFileSystemClient`（按 `host/container` 组合键），避免重复创建客户端连接。

---

## 14. 多云场景下的 FileIO 选型策略

### 14.1 单云部署方案

**AWS 环境**：直接使用 `S3FileIO`

```properties
io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

优势：
- 原生 AWS SDK 集成，支持所有 S3 特性
- 多段上传自动管理
- SSE 加密支持完善
- 支持 S3 Access Points、Transfer Acceleration
- 支持文件恢复（SupportsRecoveryOperations）
- Analytics Accelerator 加速

**GCP 环境**：直接使用 `GCSFileIO`

```properties
io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO
```

优势：
- 原生 GCS Java Client 集成
- 自动处理 Resumable Upload
- 支持客户端加解密
- 支持 Service Account Impersonation
- Analytics Core 加速

**Azure 环境**：直接使用 `ADLSFileIO`

```properties
io-impl=org.apache.iceberg.azure.adlsv2.ADLSFileIO
```

优势：
- 原生 Azure SDK 集成
- 支持多种认证方式（SAS Token、SharedKey、OAuth2、自定义 Provider）
- ADLS Gen2 Hierarchical Namespace 支持

**HDFS / 本地文件系统**：使用 `HadoopFileIO`

```properties
io-impl=org.apache.iceberg.hadoop.HadoopFileIO
```

优势：
- 无额外依赖
- 支持所有 Hadoop FileSystem 实现（HDFS、S3A、GCS Connector 等）
- 数据本地性信息（block locations）

### 14.2 混合云/多云部署

**ResolvingFileIO -- 推荐方案**

```properties
io-impl=org.apache.iceberg.io.ResolvingFileIO
```

ResolvingFileIO 是多云场景的最佳选择：
- 自动根据 URI scheme 选择底层 FileIO
- 支持 fallback 机制
- 透传 Hadoop Configuration 和 Storage Credentials
- 所有云存储 jar 需要在 classpath 中

使用场景：
- 表数据在 S3，元数据在 HDFS
- 跨云迁移过渡期（新数据写 GCS，旧数据在 S3）
- 灾备场景（主 S3，备 ADLS）

### 14.3 选型决策树

```
是否只使用一个云存储？
├── 是 → 使用对应的原生 FileIO（S3FileIO / GCSFileIO / ADLSFileIO）
│       └── 这提供了最佳的性能和特性支持
└── 否 → 是否所有存储都在 classpath 中？
    ├── 是 → 使用 ResolvingFileIO
    │       └── 自动路由，最简单的配置
    └── 否 → 使用 HadoopFileIO
            └── 通过 Hadoop FileSystem 插件支持多种存储
```

**HadoopFileIO vs 原生 FileIO 的权衡**：

| 维度 | HadoopFileIO | 原生 FileIO |
|------|-------------|------------|
| 性能 | 一般（经过 Hadoop 抽象层） | 最优（直接使用云 SDK） |
| 功能 | 基础 CRUD | 完整特性（SSE、Access Points 等） |
| 依赖 | 需要 Hadoop | 需要对应云 SDK |
| 序列化 | 需要处理 Hadoop Configuration | 配置简单 |
| 凭证管理 | 依赖 Hadoop 凭证链 | 原生凭证刷新 |
| 批量删除 | 逐个删除（并行化） | 原生批量 API |

### 14.4 迁移策略

从 HadoopFileIO 迁移到原生 FileIO 时，需要注意：

1. **配置映射**：Hadoop 配置属性（如 `fs.s3a.access.key`）需要转换为 Iceberg 属性（如 `s3.access-key-id`）
2. **URI Scheme 兼容**：S3FileIO 同时支持 `s3://`、`s3a://`、`s3n://` 三种 scheme
3. **凭证切换**：从 Hadoop 凭证链迁移到原生 AWS/GCP/Azure 凭证
4. **渐进式迁移**：可以先使用 ResolvingFileIO，让新数据使用原生 FileIO，旧数据仍通过 HadoopFileIO 访问

---

## 15. 总结

Apache Iceberg 的 FileIO 抽象体系是一个精心设计的、面向多云存储的文件访问框架。通过本文的深度分析，我们可以总结出以下关键要点：

### 架构设计

1. **接口体系**：`FileIO` -> `InputFile` / `OutputFile` -> `SeekableInputStream` / `PositionOutputStream` 形成了层次清晰的抽象。扩展接口（`DelegateFileIO`、`SupportsPrefixOperations`、`SupportsBulkOperations`、`RangeReadable`）通过 mixin 模式按需组合能力。

2. **存储无关性**：上层代码通过统一的 FileIO 接口操作文件，完全不感知底层存储系统。这使得 Iceberg 可以无缝地支持新的存储系统。

3. **装饰器模式**：`EncryptingFileIO` 叠加加密能力，`ResolvingFileIO` 叠加路由能力，而不需要修改任何底层实现。

### 实现质量

1. **资源管理**：所有流实现都通过 `finalize()` 方法检测资源泄露，并在构造时捕获调用堆栈用于诊断。所有 close 方法都使用 `AtomicBoolean` 保证并发安全。

2. **序列化支持**：通过 `SerializableConfiguration`、`SerializableMap`、`SerializableSupplier` 等工具类解决了 Java 序列化和 Kryo 序列化的兼容性问题。

3. **错误处理**：S3InputStream 使用 Failsafe 自动重试网络异常；ADLSInputStream 将 Azure 特有异常转换为 Iceberg 统一异常；所有实现都正确处理了 Not Found 情况。

4. **性能优化**：S3InputStream 的 lazy seek 和 Read-through skip 优化；S3OutputStream 的智能多段上传策略；ContentCache 的文件级缓存；批量删除的并行化处理。

### 凭证管理

1. **StorageCredential 体系**：基于前缀匹配的凭证模型，支持多账户、多区域的凭证管理。

2. **自动刷新**：S3、GCS、ADLS 都支持凭证自动刷新，提前 5 分钟预刷新，避免长时间运行的任务因凭证过期而失败。

3. **Vended Credentials**：通过 REST Catalog 的 credentials 端点下发短期凭证，实现了安全的凭证注入机制。

### 选型建议

- **单云部署**：优先使用对应的原生 FileIO 实现
- **多云部署**：使用 ResolvingFileIO 自动路由
- **Hadoop 生态兼容**：使用 HadoopFileIO 作为通用方案
- **安全敏感场景**：结合 EncryptingFileIO 实现端到端加密

FileIO 抽象层是 Iceberg 成为事实标准开放表格式的关键设计之一，它使得 Iceberg 能够真正做到 "一次定义表格式，处处可用"，无论底层存储如何变化。随着云存储技术的持续演进（如 S3 Express One Zone、GCS Analytics Core），Iceberg 的 FileIO 体系也在不断发展，保持着对最新存储特性的高效支持。
