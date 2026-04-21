import { LocaleSpecificConfig, DefaultTheme } from 'vitepress'
import * as data from './zh_data'

export const zh_themeConfig = <DefaultTheme.Config>
    {
        nav: [
            { text: '指南', link: data.guidLinks[0].link },
            { text: 'Issues', link: 'https://github.com/alibaba/yalantinglibs/issues' }
        ],
        sidebar: [
            { text: '指南', items: data.guidLinks, },
            { text: 'struct_pack', items: data.struct_pack_Links, },
            { text: 'struct_pb', items: data.struct_pb_Links, },
            { text: 'coro_rpc', items: data.coro_rpc_Links },
            { text: 'easylog', items: data.easylog_Links },
            { text: 'coro_http', items: data.coro_http_Links },
            { text: 'struct_xxx', items: data.struct_xxx_Links },
            { text: 'metric', items: data.metric_Links },
            { text: 'reflection', items: data.reflection_Links },
        ]
    };

export const zh_LocaleConfig = <LocaleSpecificConfig>{
    label: '简体中文',

    lang: 'zh-CN',
    title: '雅兰亭库',
    description: '中文简介待补充',

    // titleTemplate?: string | boolean
    // head?: HeadConfig[]
    themeConfig: zh_themeConfig
};
