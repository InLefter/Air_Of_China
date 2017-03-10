# Air_Of_China

a spider (or crawler) based on python3, which is to craw air data from Chinese official website(<http://106.37.208.233:20035>) developed with Silverlight from Micrsoft.And meantime it's a part of my campus SRTP project.

本项目是一个基于python3且从全国城市空气质量发布平台上的Silverlight中爬取的空气质量数据，同时也是我的一个SRTP项目中的一部分。

## INTRODUCTION

some usage examples had been included( SiteSpider.py, CityDaySpider.py, CityRealTimeSpider.py). you can know how it can be used.

项目中给出了几个函数的例子，借鉴即可了解如何使用。

Because net communication between Silverlight is wcfbin message, and there is a base64-encrypted process before data being deflated. Wcfbin message can be solved from [ernw/python-wcfbin](https://github.com/ernw/python-wcfbin) .

因为Silverlight中的小心通讯是使用wcfbin格式的，所以可以使用Github上的一个开源项目来解决这个问题[ernw/python-wcfbin](https://github.com/ernw/python-wcfbin) 。而且在通讯压缩数据前还有一个base64加密的过程。

This project is a try on python, i'm sorry fot that there maybe are some problems in language using or model designing. 

本项目是一个在python上的尝试，所以可能有一些语言使用或者模式设计上的问题，对此我很抱歉。

## LICENSE

This project is under [the MIT License](https://mit-license.org/).

Copyright © 2017

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.