##模板语法

此模板引擎是基于Java开源Html解析引擎[Jsoup](http://jsoup.org/)实现，jsoup实现了一套基于DOM,CSS和Jquery类似的选择器功能，我们扩展了此解析器，加入了一些自定义的解析操作，为数据抽取提供更加方便的操作。

我们的选择器可以由多种不同的选择器任意组合而成，所有的选择器通过通道方式链式执行，上一个选择的结果将作为下一个选择的输入。

语法结构：
```
selector1 | selector2 | selector3 | selector4 …
```

如：
```
.name .title | [href] | ~/id=(.*?)&/
```

##语法详细

### Jsoup Selector

通常作为选择器通道的第一个选择器。

Example:

```
div.masthead // 获取html对象
```

详细关于Jsoup 的选择语法请查看：

[selector-syntax](http://jsoup.org/cookbook/extracting-data/selector-syntax)

#### 其他扩展
我们这里扩展了Jsoup的查询语法，在jsoup查询语法上我们增加了如下功能：

* 查找兄弟节点（下一个）

Example:
```
.shop-name .shop-title | + .content // 获取文本内容
```

这里是查找`.shop-name .shop-title`元素的下一个兄弟节点并从该西东节点中查找`.content` 元素

* 查找兄弟节点（上一个）

Example:
```
.shop-name .shop-title | - .content // 获取文本内容
```
这里是查找`.shop-name .shop-title`元素的上一个兄弟节点并从该节点中查找`.content`元素

### Text Selector

从前面的结果中获取文本内容,通常用于获取html元素的内部文本内容。

语法关键词: `txt`

Example:
```
.shop-name .shop-title | txt // 获取文本内容
```

### Html Contenxt Selector

从前面的结果中获取html内容,通常用于获取html内容。

语法关键词: `html`

Example:
```
.shop-name .shop-title | html // 获取html内容
```
### Attribute Selector

从前面的结果中获取属性值,通常用于获取html元素的属性值。

**语法**: `[attributeName]`

Example:
```
.shop-name .shop-title|[itemprop] // 获取属性itemprop的值
```
### Array Selector

从前面的集合结果中获取第i 项的内容，此选择器要求前面的选择结果是一个数组或集合。如果不是数组

或集合该选择器将不起作用，直接返回前一个选择器的结果。

语法: `[index]`

Example:
```
.shop-name .shop-title|[0] // 获取第一个对象
```
### Regex Selector

对前面的选择结果执行正则表达式，并返回匹配的项作为集合返回，如果前面的选择结果不能匹配此正则

表达式，直接返回前一个选择器的结果。通常该选择器后是一个数组选择器，获取正则表达式匹配的结果。

返回集合结果和标准正则表达式的goup输出一致，第0项是整个匹配字符串，第1项是第一个匹配值（第一个（）包含的内部值）…

语法:` ~/ regex /`

Example:
```
.block-inner .first a |link|~/shop/(\\d+)/([a-zA-Z0-9_]+)/|[1] // 获取该link 中匹配正则表达式结果中第一个值
```


### Filter Selector

对前面的选择结果应用一个过滤规则，以方便进一步的选择。该选择器期望的输入是一个jsoup的输出。

语法:  `:[selector=string]`

其中的selector是一个嵌套的jsoup选择器，此过滤选择器将嵌套的jsoup选择结果的文本内容和期望字符串

进行匹配，匹配成功的结果集将作为下一个选择的输入

Example:
```
.desc-list dl|:[dt=服务]|dd em|txt
```
// 过滤包含dt元素，且内容是“服务”的dl元素，并从匹配的结果集中执行后续选择操作。

样例html如：
``` html
<dl>
 <dt>服务:</dt>
 <dd>
   <span class="progress-bar">
    <span class="bar" style="width: 100%;">100%</span>
   </span>
   <em class="progress-value">25</em>
 </dd>
</dl>
```

### Format Selector

执行格式化输出操作，该Selector适用于需要组合多个参数并格式化输出的场合。

语法:  `{{0}_sdadd_{name}_{id}}`

该Selector的使用参数可以是前面通道的输出或是上下文中已经解析出的变量。

* {index_num}，从前面通道中获取值，如果是数组就获取数组对应的索引的值，这里是获取第一个的值

* {var_name}, 从当前上下文中获取变量的值

Example:
```
#id|txt|{{0}_{postid}}
```
从文档中选取id 是id的元素的文本内容并和上下文中的postid变量进行合并。

