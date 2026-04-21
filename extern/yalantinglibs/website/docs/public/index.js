var lang = null;

function language() {
    var language = null;
    if (navigator.appName == 'Netscape') {
        language = navigator.language;
    } else {
        language = navigator.browserLanguage;
    }
    if (language.indexOf('zh') > -1) {
        lang = "zh"
        window.location.href = "/yalantinglibs/zh/index.html";
    } else {
        window.location.href = "/yalantinglibs/en/index.html";
    }
}


language()