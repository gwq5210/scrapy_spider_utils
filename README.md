## 打包

- 确保我们已安装最新setuptools 和 wheel和twine ，下面是安装/更新命令

```sh
python3 -m pip install --user --upgrade setuptools wheel twine
```

- 打包的我们的库/项目

```sh
python3 -m
```

此时在当前目录我们会看到以下：

dist/
  example_pkg_your_username-0.0.1-py3-none-any.whl
  example_pkg_your_username-0.0.1.tar.gz

- 使用 twine 将打包好的库/项目上传到PYPI

（需用到PYPI帐号密码）（此时只是上传到PYPI测试服，还不能 pip install 这个库/项目）

```sh
python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```


我们会看到如下界面：

```sh
Uploading distributions to https://test.pypi.org/legacy/
Enter your username: [your username]
Enter your password:
Uploading example_pkg_your_username-0.0.1-py3-none-any.whl
100%|█████████████████████| 4.65k/4.65k [00:01<00:00, 2.88kB/s]
Uploading example_pkg_your_username-0.0.1.tar.gz
100%|█████████████████████| 4.25k/4.25k [00:01<00:00, 3.05kB/s]
```

上传成功之后，我们可以去PYPI的测试服查看是否上传成功，能上传成功的话就说明肯定也能成功上传到PYPI正式服（附：PYPI测试服地址）

PYPI测试服的管理员会不定期删除上边的库，正式投入使用还是得上传到正式服。

由于我先前有上传库到测试服，我们可以尝试搜索看看

若是想测试下上传到测试服的库能否使用，可以使用如下命令

```sh
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps example-pkg-your-username
```

大致会出现以下：

```sh
Collecting example-pkg-your-username
  Downloading https://test-files.pythonhosted.org/packages/.../example-pkg-your-username-0.0.1-py3-none-any.whl
Installing collected packages: example-pkg-your-username
Successfully installed example-pkg-your-username-0.0.1
```

若是从测试服安装的我们的库能正常使用，那么我们就可以开始行动把它上传到PYPI正式服供大家使用了。（之所以特地提出这一步，是因为第一次上传库时，我们总会因为目录结构不会、未打包成库可正常使用打包了却不能用等等原因导致上传的是个“失败的库”，这样能避免别人会安装到我们的失败库）

简单测试是否能正常使用直接如下即可，但具体里边的功能能否正常用我们还需调用一下，此处不做介绍

```python3
>>> import example_pkg
>>> example_pkg.name
'example_pkg'
```

- 【重头戏】将库上传到 PYPI正式服

twine upload dist/*
上传成功后该库即可直接pip安装

上传PYPI示例库
LZC6244/maida
​github.com/LZC6244/maida

如果对目录结构或者其他有什么不清楚的可以参考我这个库（结构较简单适合初学者），或者我们平时使用的库（譬如本人平时经常使用 scrapy 也可以去 scrapy 主页参考大佬的写法）
