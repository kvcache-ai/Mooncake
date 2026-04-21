import {defineConfig} from "vitepress";

import {en_LocaleConfig} from "./en_locale";
import {zh_LocaleConfig} from "./zh_locale";

export default defineConfig(
    {
        //Resource
        srcDir: "docs",
        outDir: "dist",
        //Website
        base: '/yalantinglibs/',
        //Article
        lastUpdated: true,
        ignoreDeadLinks: false,

        head: [],
        //Common theme config
        themeConfig: {
            logo: '/icon/logo.svg',
            socialLinks: [
                {icon: 'github', link: 'https://github.com/alibaba/yalantinglibs'}
            ],
            footer: {
                message: 'This website is released under the MIT License.',
                copyright: 'Copyright © 2022 yalantinglibs contributors'
            },
        },
        //Locales config and theme-specify
        locales: {
            'en': <any>en_LocaleConfig,
            'zh': <any>zh_LocaleConfig,
        },
    }
)
