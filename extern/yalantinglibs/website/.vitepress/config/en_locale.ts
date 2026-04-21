import {LocaleSpecificConfig, DefaultTheme} from 'vitepress';
import * as data from './en_data'

export const en_themeConfig = <DefaultTheme.Config>
    {
        nav: [
            {text: 'Guide', link: data.guidLinks[0].link},
            {text: 'Issues', link: 'https://github.com/alibaba/yalantinglibs/issues'}
        ],
        sidebar: [
            {text: 'guide', items: data.guidLinks,},
            {text: 'struct_pack', items: data.struct_pack_Links,},
            {text: 'struct_pb', items: data.struct_pb_Links,},
            {text: 'coro_rpc', items: data.coro_rpc_Links},
        ]
    };

export const en_LocaleConfig = <LocaleSpecificConfig>{
    label: 'english',

    lang: 'en-US',
    title: 'yalantinglibs',
    description: 'A collection of C++20 libraries, include async_simple, coro_rpc and struct_pack.',

    // titleTemplate?: string | boolean
    // head?: HeadConfig[]
    themeConfig:en_themeConfig,
};


