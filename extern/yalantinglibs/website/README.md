# Build Website

The website is built by vitepress and doxygen
and you can build the website in your own computer

Before build the website,
`git`, `yarn`, `node` are required.


- clone the repo

```
git clone https://github.com/alibaba/yalantinglibs.git
```

- checkout `doc` branch

```
cd yalantinglibs
git checkout doc
```

- clone `doxygen-awesome-css` (for Doxygen)

```
https://github.com/jothepro/doxygen-awesome-css.git
```

- install dependencies for vitepress


```
yarn install
```

- generate website

```
bash generate.sh
```

the script will copy all markdown files and images to website folder

then use `yarn docs:build` and `doxygen Doxyfile` generate html


some logs
```
yarn run v1.22.19
$ vitepress build website
vitepress v1.0.0-alpha.29
✓ building client + server bundles...
⠋ rendering pages...(node:77603) ExperimentalWarning: The Fetch API is an experimental feature. This feature could change at any time
(Use `node --trace-warnings ...` to show where the warning was created)
✓ rendering pages...
build complete in 11.86s.
✨  Done in 12.95s.
Doxygen version used: 1.9.6
...
...
...
Running plantuml with JAVA...
type lookup cache used 63/65536 hits=63 misses=63
symbol lookup cache used 106/65536 hits=1267 misses=106
finished...
Done!
```

the config of vitepress is `website/.vitepress/config/index.ts`

the config of doxygen is `Doxyfile` and `Doxyfile_cn`
