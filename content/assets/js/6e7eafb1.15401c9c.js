"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[96668],{3905:(e,t,a)=>{a.d(t,{Zo:()=>u,kt:()=>m});var i=a(67294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,i)}return a}function r(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,i,n=function(e,t){if(null==e)return{};var a,i,n={},o=Object.keys(e);for(i=0;i<o.length;i++)a=o[i],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(i=0;i<o.length;i++)a=o[i],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var l=i.createContext({}),d=function(e){var t=i.useContext(l),a=t;return e&&(a="function"==typeof e?e(t):r(r({},t),e)),a},u=function(e){var t=d(e.components);return i.createElement(l.Provider,{value:t},e.children)},c="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},h=i.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,l=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),c=d(a),h=n,m=c["".concat(l,".").concat(h)]||c[h]||p[h]||o;return a?i.createElement(m,r(r({ref:t},u),{},{components:a})):i.createElement(m,r({ref:t},u))}));function m(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,r=new Array(o);r[0]=h;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s[c]="string"==typeof e?e:n,r[1]=s;for(var d=2;d<o;d++)r[d]=a[d];return i.createElement.apply(null,r)}return i.createElement.apply(null,a)}h.displayName="MDXCreateElement"},62727:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var i=a(87462),n=(a(67294),a(3905));const o={title:"General",keywords:["hudi","writing","reading"]},r="General FAQ",s={unversionedId:"faq_general",id:"version-0.14.1/faq_general",title:"General",description:"When is Hudi useful for me or my organization?",source:"@site/versioned_docs/version-0.14.1/faq_general.md",sourceDirName:".",slug:"/faq_general",permalink:"/docs/faq_general",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.14.1/faq_general.md",tags:[],version:"0.14.1",frontMatter:{title:"General",keywords:["hudi","writing","reading"]},sidebar:"docs",previous:{title:"Overview",permalink:"/docs/faq"},next:{title:"Design & Concepts",permalink:"/docs/faq_design_and_concepts"}},l=[{value:"When is Hudi useful for me or my organization?",id:"when-is-hudi-useful-for-me-or-my-organization",children:[],level:3},{value:"What are some non-goals for Hudi?",id:"what-are-some-non-goals-for-hudi",children:[],level:3},{value:"What is incremental processing? Why does Hudi docs/talks keep talking about it?",id:"what-is-incremental-processing-why-does-hudi-docstalks-keep-talking-about-it",children:[],level:3},{value:"How is Hudi optimized for CDC and streaming use cases?",id:"how-is-hudi-optimized-for-cdc-and-streaming-use-cases",children:[],level:3},{value:"How do I choose a storage type for my workload?",id:"how-do-i-choose-a-storage-type-for-my-workload",children:[],level:3},{value:"Is Hudi an analytical database?",id:"is-hudi-an-analytical-database",children:[],level:3},{value:"How do I model the data stored in Hudi?",id:"how-do-i-model-the-data-stored-in-hudi",children:[],level:3},{value:"Why does Hudi require a key field to be configured?",id:"why-does-hudi-require-a-key-field-to-be-configured",children:[],level:3},{value:"How does Hudi actually store data inside a table?",id:"how-does-hudi-actually-store-data-inside-a-table",children:[],level:3},{value:"How Hudi handles partition evolution requirements ?",id:"how-hudi-handles-partition-evolution-requirements-",children:[],level:3}],d={toc:l},u="wrapper";function c(e){let{components:t,...a}=e;return(0,n.kt)(u,(0,i.Z)({},d,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"general-faq"},"General FAQ"),(0,n.kt)("h3",{id:"when-is-hudi-useful-for-me-or-my-organization"},"When is Hudi useful for me or my organization?"),(0,n.kt)("p",null,"If you are looking to quickly ingest data onto HDFS or cloud storage, Hudi can provide you tools to ",(0,n.kt)("a",{parentName:"p",href:"/docs/writing_data/"},"help"),". Also, if you have ETL/hive/spark jobs which are slow/taking up a lot of resources, Hudi can potentially help by providing an incremental approach to reading and writing data."),(0,n.kt)("p",null,"As an organization, Hudi can help you build an ",(0,n.kt)("a",{parentName:"p",href:"https://docs.google.com/presentation/d/1FHhsvh70ZP6xXlHdVsAI0g__B_6Mpto5KQFlZ0b8-mM/edit#slide=id.p"},"efficient data lake"),", solving some of the most complex, low-level storage management problems, while putting data into hands of your data analysts, engineers and scientists much quicker."),(0,n.kt)("h3",{id:"what-are-some-non-goals-for-hudi"},"What are some non-goals for Hudi?"),(0,n.kt)("p",null,"Hudi is not designed for any OLTP use-cases, where typically you are using existing NoSQL/RDBMS data stores. Hudi cannot replace your in-memory analytical database (at-least not yet!). Hudi support near-real time ingestion in the order of few minutes, trading off latency for efficient batching. If you truly desirable sub-minute processing delays, then stick with your favorite stream processing solution."),(0,n.kt)("h3",{id:"what-is-incremental-processing-why-does-hudi-docstalks-keep-talking-about-it"},"What is incremental processing? Why does Hudi docs/talks keep talking about it?"),(0,n.kt)("p",null,"Incremental processing was first introduced by Vinoth Chandar, in the O'reilly ",(0,n.kt)("a",{parentName:"p",href:"https://www.oreilly.com/content/ubers-case-for-incremental-processing-on-hadoop/"},"blog"),", that set off most of this effort. In purely technical terms, incremental processing merely refers to writing mini-batch programs in streaming processing style. Typical batch jobs consume ",(0,n.kt)("strong",{parentName:"p"},"all input")," and recompute ",(0,n.kt)("strong",{parentName:"p"},"all output"),", every few hours. Typical stream processing jobs consume some ",(0,n.kt)("strong",{parentName:"p"},"new input")," and recompute ",(0,n.kt)("strong",{parentName:"p"},"new/changes to output"),", continuously/every few seconds. While recomputing all output in batch fashion can be simpler, it's wasteful and resource expensive. Hudi brings ability to author the same batch pipelines in streaming fashion, run every few minutes."),(0,n.kt)("p",null,"While we can merely refer to this as stream processing, we call it ",(0,n.kt)("em",{parentName:"p"},"incremental processing"),", to distinguish from purely stream processing pipelines built using Apache Flink or Apache Kafka Streams."),(0,n.kt)("h3",{id:"how-is-hudi-optimized-for-cdc-and-streaming-use-cases"},"How is Hudi optimized for CDC and streaming use cases?"),(0,n.kt)("p",null,"One of the core use-cases for Apache Hudi is enabling seamless, efficient database ingestion to your lake, and change data capture is a direct application of that. Hudi\u2019s core design primitives support fast upserts and deletes of data that are suitable for CDC and streaming use cases. Here is a glimpse of some of the challenges accompanying streaming and cdc workloads that Hudi handles efficiently out of the box."),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("em",{parentName:"strong"},"Processing of deletes:"))," Deletes are treated no differently than updates and are logged with the same filegroups where the corresponding keys exist. This helps process deletes faster same like regular inserts and updates and Hudi processes deletes at file group level using compaction in MOR tables. This can be very expensive in other open source systems that store deletes as separate files than data files and incur N(Data files)","*","N(Delete files) merge cost to process deletes every time, soon lending into a complex graph problem to solve whose planning itself is expensive. This gets worse with volume, especially when dealing with CDC style workloads that streams changes to records frequently."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("em",{parentName:"strong"},"Operational overhead of merging deletes at scale:"))," When deletes are stored as separate files without any notion of data locality, the merging of data and deletes can become a run away job that cannot complete in time due to various reasons (Spark retries, executor failure, OOM, etc.). As more data files and delete files are added, the merge becomes even more expensive and complex later on, making it hard to manage in practice causing operation overhead. Hudi removes this complexity from users by treating deletes similarly to any other write operation."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("em",{parentName:"strong"},"File sizing with updates:"))," Other open source systems, process updates by generating new data files for inserting the new records after deletion, where both data files and delete files get introduced for every batch of updates. This yields to small file problem and requires file sizing. Whereas, Hudi embraces mutations to the data, and manages the table automatically by keeping file sizes in check without passing the burden of file sizing to users as manual maintenance."),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("strong",{parentName:"li"},(0,n.kt)("em",{parentName:"strong"},"Support for partial updates and payload ordering:"))," Hudi support partial updates where already existing record can be updated for specific fields that are non null from newer records (with newer timestamps). Similarly, Hudi supports payload ordering with timestamp through specific payload implementation where late-arriving data with older timestamps will be ignored or dropped. Users can even implement custom logic and plug in to handle what they want.")),(0,n.kt)("h3",{id:"how-do-i-choose-a-storage-type-for-my-workload"},"How do I choose a storage type for my workload?"),(0,n.kt)("p",null,"A key goal of Hudi is to provide ",(0,n.kt)("strong",{parentName:"p"},"upsert functionality")," that is orders of magnitude faster than rewriting entire tables or partitions."),(0,n.kt)("p",null,"Choose Copy-on-write storage if :"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"You are looking for a simple alternative, that replaces your existing parquet tables without any need for real-time data."),(0,n.kt)("li",{parentName:"ul"},"Your current job is rewriting entire table/partition to deal with updates, while only a few files actually change in each partition."),(0,n.kt)("li",{parentName:"ul"},"You are happy keeping things operationally simpler (no compaction etc), with the ingestion/write performance bound by the ",(0,n.kt)("a",{parentName:"li",href:"/docs/configurations#hoodieparquetmaxfilesize"},"parquet file size")," and the number of such files affected/dirtied by updates"),(0,n.kt)("li",{parentName:"ul"},"Your workload is fairly well-understood and does not have sudden bursts of large amount of update or inserts to older partitions. COW absorbs all the merging cost on the writer side and thus these sudden changes can clog up your ingestion and interfere with meeting normal mode ingest latency targets.")),(0,n.kt)("p",null,"Choose merge-on-read storage if :"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"You want the data to be ingested as quickly & queryable as much as possible."),(0,n.kt)("li",{parentName:"ul"},"Your workload can have sudden spikes/changes in pattern (e.g bulk updates to older transactions in upstream database causing lots of updates to old partitions on DFS). Asynchronous compaction helps amortize the write amplification caused by such scenarios, while normal ingestion keeps up with incoming stream of changes.")),(0,n.kt)("p",null,"Immaterial of what you choose, Hudi provides"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Snapshot isolation and atomic write of batch of records"),(0,n.kt)("li",{parentName:"ul"},"Incremental pulls"),(0,n.kt)("li",{parentName:"ul"},"Ability to de-duplicate data")),(0,n.kt)("p",null,"Find more ",(0,n.kt)("a",{parentName:"p",href:"/docs/concepts/"},"here"),"."),(0,n.kt)("h3",{id:"is-hudi-an-analytical-database"},"Is Hudi an analytical database?"),(0,n.kt)("p",null,"A typical database has a bunch of long running storage servers always running, which takes writes and reads. Hudi's architecture is very different and for good reasons. It's highly decoupled where writes and queries/reads can be scaled independently to be able to handle the scale challenges. So, it may not always seems like a database."),(0,n.kt)("p",null,"Nonetheless, Hudi is designed very much like a database and provides similar functionality (upserts, change capture) and semantics (transactional writes, snapshot isolated reads)."),(0,n.kt)("h3",{id:"how-do-i-model-the-data-stored-in-hudi"},"How do I model the data stored in Hudi?"),(0,n.kt)("p",null,"When writing data into Hudi, you model the records like how you would on a key-value store - specify a key field (unique for a single partition/across table), a partition field (denotes partition to place key into) and preCombine/combine logic that specifies how to handle duplicates in a batch of records written. This model enables Hudi to enforce primary key constraints like you would get on a database table. See ",(0,n.kt)("a",{parentName:"p",href:"/docs/writing_data/"},"here")," for an example."),(0,n.kt)("p",null,"When querying/reading data, Hudi just presents itself as a json-like hierarchical table, everyone is used to querying using Hive/Spark/Presto over Parquet/Json/Avro."),(0,n.kt)("h3",{id:"why-does-hudi-require-a-key-field-to-be-configured"},"Why does Hudi require a key field to be configured?"),(0,n.kt)("p",null,"Hudi was designed to support fast record level Upserts and thus requires a key to identify whether an incoming record is\nan insert or update or delete, and process accordingly. Additionally, Hudi automatically maintains indexes on this primary\nkey and for many use-cases like CDC, ensuring such primary key constraints is crucial to ensure data quality. In this context,\npre combine key helps reconcile multiple records with same key in a single batch of input records. Even for append-only data\nstreams, Hudi supports key based de-duplication before inserting records. For e-g; you may have atleast once data integration\nsystems like Kafka MirrorMaker that can introduce duplicates during failures. Even for plain old batch pipelines, keys\nhelp eliminate duplication that could be caused by backfill pipelines, where commonly it's unclear what set of records\nneed to be re-written. We are actively working on making keys easier by only requiring them for Upsert and/or automatically\ngenerate the key internally (much like RDBMS row","_","ids)"),(0,n.kt)("h3",{id:"how-does-hudi-actually-store-data-inside-a-table"},"How does Hudi actually store data inside a table?"),(0,n.kt)("p",null,"At a high level, Hudi is based on MVCC design that writes data to versioned parquet/base files and log files that contain changes to the base file. All the files are stored under a partitioning scheme for the table, which closely resembles how Apache Hive tables are laid out on DFS. Please refer ",(0,n.kt)("a",{parentName:"p",href:"/docs/concepts/"},"here")," for more details."),(0,n.kt)("h3",{id:"how-hudi-handles-partition-evolution-requirements-"},"How Hudi handles partition evolution requirements ?"),(0,n.kt)("p",null,"Hudi recommends keeping coarse grained top level partition paths e.g date(ts) and within each such partition do clustering in a flexible way to z-order, sort data based on interested columns. This provides excellent performance by : minimzing the number of files in each partition, while still packing data that will be queried together physically closer (what partitioning aims to achieve)."),(0,n.kt)("p",null,"Let's take an example of a table, where we store log","_","events with two fields ",(0,n.kt)("inlineCode",{parentName:"p"},"ts")," (time at which event was produced) and ",(0,n.kt)("inlineCode",{parentName:"p"},"cust_id")," (user for which event was produced) and a common option is to partition by both date(ts) and cust","_","id.\nSome users may want to start granular with hour(ts) and then later evolve to new partitioning scheme say date(ts). But this means, the number of partitions in the table could be very high - 365 days x 1K customers = at-least 365K potentially small parquet files, that can significantly slow down queries, facing throttling issues on the actual S3/DFS reads."),(0,n.kt)("p",null,"For the afore mentioned reasons, we don't recommend mixing different partitioning schemes within the same table, since it adds operational complexity, and unpredictable performance.\nOld data stays in old partitions and only new data gets into newer evolved partitions. If you want to tidy up the table, one has to rewrite all partition/data anwyay! This is where we suggest start with coarse grained partitions\nand lean on clustering techniques to optimize for query performance."),(0,n.kt)("p",null,"We find that most datasets have at-least one high fidelity field, that can be used as a coarse partition. Clustering strategies in Hudi provide a lot of power - you can alter which partitions to cluster, and which fields to cluster each by etc.\nUnlike Hive partitioning, Hudi does not remove the partition field from the data files i.e if you write new partition paths, it does not mean old partitions need to be rewritten.\nPartitioning by itself is a relic of the Hive era; Hudi is working on replacing partitioning with database like indexing schemes/functions,\nfor even more flexibility and get away from Hive-style partition evol route."))}c.isMDXComponent=!0}}]);