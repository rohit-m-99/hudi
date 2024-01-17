"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[95361],{3905:(e,t,r)=>{r.d(t,{Zo:()=>d,kt:()=>g});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var u=n.createContext({}),l=function(e){var t=n.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},d=function(e){var t=l(e.components);return n.createElement(u.Provider,{value:t},e.children)},p="mdxType",c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},h=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,u=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),p=l(r),h=a,g=p["".concat(u,".").concat(h)]||p[h]||c[h]||i;return r?n.createElement(g,o(o({ref:t},d),{},{components:r})):n.createElement(g,o({ref:t},d))}));function g(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=h;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s[p]="string"==typeof e?e:a,o[1]=s;for(var l=2;l<i;l++)o[l]=r[l];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}h.displayName="MDXCreateElement"},93484:(e,t,r)=>{r.r(t),r.d(t,{contentTitle:()=>o,default:()=>p,frontMatter:()=>i,metadata:()=>s,toc:()=>u});var n=r(87462),a=(r(67294),r(3905));const i={title:"Integrations",keywords:["hudi","writing","reading"]},o="Integrations FAQ",s={unversionedId:"faq_integrations",id:"version-0.14.1/faq_integrations",title:"Integrations",description:"Does AWS GLUE support Hudi ?",source:"@site/versioned_docs/version-0.14.1/faq_integrations.md",sourceDirName:".",slug:"/faq_integrations",permalink:"/docs/faq_integrations",editUrl:"https://github.com/apache/hudi/tree/asf-site/website/versioned_docs/version-0.14.1/faq_integrations.md",tags:[],version:"0.14.1",frontMatter:{title:"Integrations",keywords:["hudi","writing","reading"]},sidebar:"docs",previous:{title:"Storage",permalink:"/docs/faq_storage"},next:{title:"Use Cases",permalink:"/docs/use_cases"}},u=[{value:"Does AWS GLUE support Hudi ?",id:"does-aws-glue-support-hudi-",children:[],level:3},{value:"How to override Hudi jars in EMR?",id:"how-to-override-hudi-jars-in-emr",children:[],level:3}],l={toc:u},d="wrapper";function p(e){let{components:t,...r}=e;return(0,a.kt)(d,(0,n.Z)({},l,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"integrations-faq"},"Integrations FAQ"),(0,a.kt)("h3",{id:"does-aws-glue-support-hudi-"},"Does AWS GLUE support Hudi ?"),(0,a.kt)("p",null,'AWS Glue jobs can write, read and update Glue Data Catalog for hudi tables. In order to successfully integrate with Glue Data Catalog, you need to subscribe to one of the AWS provided Glue connectors named "AWS Glue Connector for Apache Hudi". Glue job needs to have "Use Glue data catalog as the Hive metastore" option ticked. Detailed steps with a sample scripts is available on this article provided by AWS - ',(0,a.kt)("a",{parentName:"p",href:"https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/"},"https://aws.amazon.com/blogs/big-data/writing-to-apache-hudi-tables-using-aws-glue-connector/"),"."),(0,a.kt)("p",null,"In case if your using either notebooks or Zeppelin through Glue dev-endpoints, your script might not be able to integrate with Glue DataCatalog when writing to hudi tables."),(0,a.kt)("h3",{id:"how-to-override-hudi-jars-in-emr"},"How to override Hudi jars in EMR?"),(0,a.kt)("p",null,"If you are looking to override Hudi jars in your EMR clusters one way to achieve this is by providing the Hudi jars through a bootstrap script."),(0,a.kt)("p",null,"Here are the example steps for overriding Hudi version 0.7.0 in EMR 0.6.2."),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Build Hudi Jars:")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"# Git clone\ngit clone https://github.com/apache/hudi.git && cd hudi   \n\n# Get version 0.7.0\ngit checkout --track origin/release-0.7.0\n\n# Build jars with spark 3.0.0 and scala 2.12 (since emr 6.2.0 uses spark 3 which requires scala 2.12):\nmvn clean package -DskipTests -Dspark3  -Dscala-2.12 -T 30 \n")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Copy jars to s3:")),(0,a.kt)("p",null,"These are the jars we are interested in after build completes. Copy them to a temp location first."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"mkdir -p ~/Downloads/hudi-jars\ncp packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-0.7.0.jar ~/Downloads/hudi-jars/\ncp packaging/hudi-hive-sync-bundle/target/hudi-hive-sync-bundle-0.7.0.jar ~/Downloads/hudi-jars/\ncp packaging/hudi-spark-bundle/target/hudi-spark-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/\ncp packaging/hudi-timeline-server-bundle/target/hudi-timeline-server-bundle-0.7.0.jar ~/Downloads/hudi-jars/\ncp packaging/hudi-utilities-bundle/target/hudi-utilities-bundle_2.12-0.7.0.jar ~/Downloads/hudi-jars/\n")),(0,a.kt)("p",null,"Upload all jars from ~/Downloads/hudi-jars/ to the s3 location s3://xxx/yyy/hudi-jars"),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Include Hudi jars as part of the emr bootstrap script:")),(0,a.kt)("p",null,"Below script downloads Hudi jars from above s3 location. Use this script as part ",(0,a.kt)("inlineCode",{parentName:"p"},"bootstrap-actions")," when launching the EMR cluster to install the jars in each node."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-bash"},"#!/bin/bash\nsudo mkdir -p /mnt1/hudi-jars\n\nsudo aws s3 cp s3://xxx/yyy/hudi-jars /mnt1/hudi-jars --recursive\n\n# create symlinks\ncd /mnt1/hudi-jars\nsudo ln -sf hudi-hadoop-mr-bundle-0.7.0.jar hudi-hadoop-mr-bundle.jar\nsudo ln -sf hudi-hive-sync-bundle-0.7.0.jar hudi-hive-sync-bundle.jar\nsudo ln -sf hudi-spark-bundle_2.12-0.7.0.jar hudi-spark-bundle.jar\nsudo ln -sf hudi-timeline-server-bundle-0.7.0.jar hudi-timeline-server-bundle.jar\nsudo ln -sf hudi-utilities-bundle_2.12-0.7.0.jar hudi-utilities-bundle.jar\n")),(0,a.kt)("p",null,(0,a.kt)("strong",{parentName:"p"},"Using the overriden jar in Deltastreamer:")),(0,a.kt)("p",null,"When invoking DeltaStreamer specify the above jar location as part of spark-submit command."))}p.isMDXComponent=!0}}]);