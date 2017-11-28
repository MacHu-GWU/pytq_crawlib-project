#!/usr/bin/env python
# -*- coding: utf-8 -*-

import six
import time
import attr
import diskcache
import mongoengine
from datetime import datetime, timedelta
from sfm.exception_mate import get_last_exc_info
from attrs_mate import AttrsClass
from pytq import MongoDBStatusFlagScheduler
from crawlib import exc, Status, decoder, requests_spider, ChromeSpider


@attr.s
class InputData(AttrsClass):
    data = attr.ib()
    request_kwargs = attr.ib(default=attr.Factory(dict))
    get_html_kwargs = attr.ib(default=attr.Factory(dict))
    parse_html_kwargs = attr.ib(default=attr.Factory(dict))
    ignore_cache = attr.ib(default=False)
    update_cache = attr.ib(default=True)
    expire = attr.ib(default=None)


@attr.s
class OutputData(AttrsClass):
    url = attr.ib(default=None)
    html = attr.ib(default=None)
    data = attr.ib(default=None)
    status = attr.ib(default=Status.S0_ToDo.id)


class BaseScheduler(MongoDBStatusFlagScheduler):
    """

    :param use_browser: if True, you have to implement ``get_html(url)`` method.
    """

    model_klass = None
    duplicate_flag = Status.S50_Finished.id
    update_interval = 24 * 3600
    cache = None
    use_requests = True
    chrome_drive_path = None

    def __init__(self, logger=None):
        if self.duplicate_flag < 0: # pragma: no cover
            raise ValueError
        if not isinstance(self.update_interval, six.integer_types): # pragma: no cover
            raise TypeError
        if (self.cache is None) or (
                not isinstance(self.cache, diskcache.Cache)): # pragma: no cover
            raise TypeError
        collection = self.model_klass._get_collection()
        super(MongoDBStatusFlagScheduler, self). \
            __init__(logger=logger, collection=collection)

        self.collection = collection
        self.col = self.collection

    def user_hash_input(self, input_data):
        doc = input_data.data
        return doc._id

    def build_url(self, doc): # pragma: no cover
        """
        :return: url.
        """
        msg = ("You have to implement this method to create url using "
               "document data. Document data is an instance of "
               "mongoengine.Document, typically it should be uniquely "
               "associated with Document._id.")
        raise NotImplementedError(msg)

    def request(self, url, **kwargs):
        """
        :return: :class:`requests.Response`.
        """
        return requests_spider.get(url, **kwargs)

    def get_html(self, url, **kwargs):
        """
        :return: str, html.
        """
        raise self.selenium_spider.get_html(url)

    def parse_html(self, html, **kwargs): # pragma: no cover
        """
        :return: :class:`crawlib.ParseResult`.

        **中文文档**

        实现这个方法的一些限制：

        1. 当从html中获得了你想要的信息，换言之该url已经抓取完成，不需要再抓取时，
        不会抛出任何异常，直接正常返回。
        2. 其中如果数据不够完整，但你觉得数据已经够用了，暂时你并不想再进行抓取，
        可能很久以后会再次访问url进行抓取时，需要赋值
        ``ParseResult(status=Status.S60_ServerSideError.id)``，然后正常返回。
        3. 如果服务器上该Url目前无法访问，换言之此时此刻无论访问url多少次都不会有数据，
        那么可以直接抛出 exc.ServerSideError的异常。Scheduler会将其标记，不对其进行抓取。
        但可能以后会对该类错误进行再次尝试。
        """
        msg = ("You have to implement this method to parse useful data "
               "from html. The returns has to be ``crawlib.ParseResult``, "
               "having two attributes, ``.kwargs`` and ``.data``. ")
        raise NotImplementedError(msg)

    def get_input_data_queue(self,
                             filters=None,
                             limit=None,
                             request_kwargs=None,
                             get_html_kwargs=None,
                             parse_html_kwargs=None,
                             ignore_cache=False,
                             update_cache=True,
                             expire=None):
        """
        :param filters: mongodb query.
        :param limit: only returns first N documents.
        :param request_kwargs: optional parameters will be used in
            ``request(url, **request_kwargs)``.
        :param get_html_kwargs: optional
        :param ignore_cache:
        :param update_cache:
        :param expire:
        :return:
        """
        if limit is 0:
            limit = None
        if filters is None:
            now = datetime.utcnow()
            n_sec_ago = now - timedelta(seconds=self.update_interval)
            filters = {
                self.status_key: {"$lt": self.duplicate_flag},
                self.edit_at_key: {"$lt": n_sec_ago},
            }
        if request_kwargs is None:
            request_kwargs = {}
        if get_html_kwargs is None:
            get_html_kwargs = {}
        if parse_html_kwargs is None:
            parse_html_kwargs = {}
        if expire is None:
            expire = self.update_interval

        input_data_queue = list()
        for model_data in self.model_klass.by_filter(filters).limit(limit):
            input_data = InputData(
                data=model_data,
                request_kwargs=request_kwargs,
                get_html_kwargs=get_html_kwargs,
                parse_html_kwargs=parse_html_kwargs,
                ignore_cache=ignore_cache,
                update_cache=update_cache,
                expire=expire,
            )
            input_data_queue.append(input_data)
        return input_data_queue

    def user_process(self, input_data):
        out = OutputData()
        doc = input_data.data
        url = self.build_url(doc)
        out.url = url

        self.info("Crawl %s ..." % url, 1)

        flag_do_request = True
        if input_data.ignore_cache is False:
            if url in self.cache:
                flag_do_request = False
                html = self.cache[url]

        if flag_do_request:
            self.info("Making real requests!", 1)
            if self.use_requests:
                try:
                    response = self.request(url, **input_data.request_kwargs)
                except:
                    msg = "Failed to make http request: %s" % get_last_exc_info()
                    self.info(msg, 1)
                    out.status = Status.S10_HttpError.id
                    return out

                if 200 <= response.status_code < 300:
                    html = decoder.decode(
                        response.content, url, encoding="utf-8")
                elif response.status_code == 403:
                    msg = ("You reach the limit, "
                           "program will sleep for 24 hours, "
                           "please wait for a day to continue...")
                    self.info(msg, 1)
                    out.status = Status.S20_WrongPage.id
                    time.sleep(24 * 3600)
                    return out
                elif response.status_code == 404:  # page not exists
                    msg = "page doesn't exists on server!"
                    self.info(msg, 1)
                    out.status = Status.S60_ServerSideError.id
                    return out

            else:
                try:
                    html = self.chrome_spider.get_html(
                        url, **input_data.get_html_kwargs)
                except:
                    msg = "Failed to make http request: %s" % get_last_exc_info()
                    self.info(msg, 1)
                    out.status = Status.S10_HttpError.id
                    return out

        out.html = html

        try:
            parse_result = self.parse_html(html, **input_data.parse_html_kwargs)
            out.data = parse_result
            msg = "Successfully extracted data!"
            self.info(msg, 1)
            if parse_result.status == Status.S60_ServerSideError:
                out.status = Status.S60_ServerSideError
            else:
                out.status = Status.S50_Finished.id
        except exc.ServerSideError:
            out.html = html
            msg = "Server side error!" % get_last_exc_info()
            self.info(msg, 1)
            out.status = Status.S60_ServerSideError.id
        except:
            msg = "Failed to parse html: %s" % get_last_exc_info()
            self.info(msg, 1)
            out.status = Status.S30_ParseError.id
        return out

    def to_dict_only_not_none_field(self, model_data):
        d = model_data.to_dict()
        d.pop("_id")
        d.pop(self.status_key)
        d.pop(self.edit_at_key)

        true_false = [True, False]
        for key, value in d.items():
            if (value not in true_false) and (bool(value) is False):
                d.pop(key)
        return d

    def do(self,
           input_data_queue,
           pre_process=None,
           multiprocess=False,
           processes=None,
           ignore_error=True):
        if self.use_requests is False:
            with ChromeSpider(self.chrome_drive_path) as chrome_spider:
                self.chrome_spider = chrome_spider
                super(BaseScheduler, self).do(
                    input_data_queue=input_data_queue,
                    pre_process=pre_process,
                    multiprocess=False,
                    processes=None,
                    ignore_error=ignore_error,
                )
        else:
            super(BaseScheduler, self).do(
                input_data_queue=input_data_queue,
                pre_process=pre_process,
                multiprocess=multiprocess,
                processes=processes,
                ignore_error=ignore_error,
            )


class OneToMany(BaseScheduler):
    child_klass = None
    n_child_key = None

    def __init__(self, logger=None):
        super(OneToMany, self).__init__(logger=logger)

        if self.child_klass is None:
            raise NotImplementedError

        if not isinstance(self.n_child_key, six.string_types):
            raise TypeError
        n_child_field = getattr(self.model_klass, self.n_child_key)
        if not isinstance(n_child_field, mongoengine.IntField):
            raise TypeError

    def user_post_process(self, task):
        input_data = task.input_data
        output_data = task.output_data

        upd = {
            self.status_key: output_data.status,
            self.edit_at_key: datetime.utcnow(),
        }
        if output_data.status >= self.duplicate_flag:
            if input_data.update_cache:
                self.cache.set(
                    output_data.url, output_data.html,
                    expire=input_data.expire,
                )
            parse_result = output_data.data
            n_child = len(parse_result.data)
            upd[self.n_child_key] = n_child

            if n_child:
                self.child_klass.smart_insert(parse_result.data)

        self.col.update({"_id": task.id}, {"$set": upd})


class OneToOne(BaseScheduler):
    def user_post_process(self, task):
        input_data = task.input_data
        output_data = task.output_data

        upd = {
            self.status_key: output_data.status,
            self.edit_at_key: datetime.utcnow(),
        }

        if output_data.status >= self.duplicate_flag:
            if input_data.update_cache:
                self.cache.set(
                    output_data.url, output_data.html,
                    expire=input_data.expire,
                )

            parse_result = output_data.data
            model_data = parse_result.data
            upd.update(self.to_dict_only_not_none_field(model_data))

        self.col.update({"_id": task.id}, {"$set": upd})
