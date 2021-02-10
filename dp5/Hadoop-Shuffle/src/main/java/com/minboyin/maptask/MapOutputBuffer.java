package com.minboyin.maptask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by minbo on 2021/2/6 10:41
 * 1、如何创建一个环形缓冲区
 * 2、如何mark缓冲区中的metadata：【类型选择】：int ，【数据结构】：
 * 3、如何mark缓冲区中的kvbuffer{key,value}:【数据类型】：byte，【数据结构】：byte[]
 * 4、如何实现spill的使用达到80%就溢出到磁盘
 * 5、缓冲区写入磁盘的方式：流传输？序列化传输？
 * ------------------------------------------------------------------------------
 * 1、环形缓冲区其实是一个数组，数组中存放着key、value的序列化数据和key、value的元数据信息
 * 2、key/value的元数据存储的格式是int类型，每个key/value对应一个元数据，元数据由4个int组成:
 *      1、第一个int存放value的起始位置，
 *      2、第二个存放key的起始位置，
 *      3、第三个存放partition，
 *      4、最后一个存放value的长度。
 *  3、key/value序列化的数据和元数据在环形缓冲区中的存储是由equator分隔的
 *      1、key/value按照索引递增的方向存储，meta则按照索引递减的方向存储【这是什么意思，为什么一个递增{顺时针}，一个递减{逆时针}】
 */

public class MapOutputBuffer {
    private static final int INDEX_CACHE_MEMORY_LIMIT_DEFAULT = 1048576;
    /*
        环形缓冲区数据结构
     */
    //存放metadata数据的都是IntBuffer，都是int entry，占4个字节
    private IntBuffer kvmeta;
    int kvstart;
    int kvend;
    int kvindex;         // marks end of fully serialized records
    // meta数据和kvbuffer{key,value}都存在同一个环形缓冲区，以下为data环形缓冲区
    // 存放key value的byte数组，单位是byte，注意与kvmeta区分
    int equator;          // marks origin of meta/serialization
    int bufstart;         // marks beginning of spill
    int bufend;           // marks beginning of collectable
    int bufmark;          // marks end of record
    int bufindex;         // marks end of collected
    int bufvoid;          // marks the point where we should stop

    byte[] kvbuffer;
    private final byte[] b0 = new byte[0];

    //{key,value}在kvbuffer中的地址存放在【偏移】kvindex的距离
    private static final int VALSTART=0;   // val offset in acct
    private static final int KEYSTART=1;   // key offset in acct

    // partition信息存放在kvmeta中【偏移】kvindex的距离
    private static final int PARTITION=2;  // partition offset in acct
    private static final int VALLEN = 3;   // length of value

    // 一对key value的meta数据在kvmeta中占用的个数
    private static final int NMETA=4;      // num meta ints
    // 一对key value的meta数据在kvmeta中占用的byte数
    private static final int METASIZE=NMETA*4;   // size in bytes

    /*
        初始化：MapOutputBuffer.init
     */
    public void init(MapOutputCollector.Context context) throws IOException {
        JobConf job = context.getJobConf();
        // MAP_SORT_SPILL_PERCENT = mapreduce.map.sort.spill.percent
        // map 端buffer所占的百分比
        // sanity checks
        final float spillper= job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);

        //map端buffer的大小
        //mapreduce.task.io.sort.mb * mapreduce.map.sort.spill.percent 最好是16的整数倍
        final int sortmb= job.getInt(JobContext.IO_SORT_MB, 100);

        //所有spill index 在内存所占的大小的阈值
        int indexCacheMemoryLimit = job.getInt(JobContext.INDEX_CACHE_MEMORY_LIMIT, INDEX_CACHE_MEMORY_LIMIT_DEFAULT);

        // 排序的实现类，可以自己实现。这里用的是改写的快排


    }


}
