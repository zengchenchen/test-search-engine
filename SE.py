import sqlite3


class Search():
    def __init__(self, db_path):
        self.con = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.con.cursor()

    def __del__(self):
        self.cursor.close()
        self.con.close()
        print('************ Attention: db execute close! ************')

    def dbcommit(self):
        self.con.commit()

    #找出所有单词同时出现在同一URL内的所有组合!!!!
    def get_match_rows(self, q):
        # 构造查询的字符串
        field_list = 'w0.url_id'
        table_list = ''
        clause_list = ''
        word_ids = []
        # 根据空格拆分单词
        words = q.lower().split()[:2]
        table_number = 0

        for word in words:
            # 获取单词的ID
            word_row = self.cursor.execute(
                "select id from word_list where word=?", (word, )).fetchone()
            if word_row != None:
                word_id = word_row[0]
                word_ids.append(word_id)
                if table_number > 0:
                    table_list += ','
                    clause_list += ' and '
                    clause_list += 'w%d.url_id=w%d.url_id and ' % (
                        table_number - 1, table_number)
                field_list += ',w%d.location' % table_number
                table_list += 'word_location w%d' % table_number
                clause_list += 'w%d.word_id=%d' % (table_number, word_id)
                table_number += 1

        # 根据各个分组，建立查询
        full_query = 'select %s from %s where %s' % (field_list, table_list,
                                                     clause_list)
        if clause_list == '':
            full_query = 'select w0.url_id,w0.location from word_location w0 where w0.word_id=1'
        cur = self.cursor.execute(full_query)
        rows = [row for row in cur]

        return rows, word_ids

    def get_scored_list(self, rows, word_ids):  #word_ids ????????????
        total_scores = dict([(row[0], 0) for row in rows])

        # 此处放置评价函数
        weights = [(1.0, self.frequency_score(rows)),
                   (1.5, self.location_score(rows)),
                   (2.0, self.distance_score(rows))]
        #         print('weights:', weights)
        for weight, scores in weights:
            for url in total_scores:
                total_scores[url] += weight * scores[url]
        return total_scores

    def get_url_name(self, id):
        return self.cursor.execute("select url from url_list where id=?",
                                   (id, )).fetchone()[0]

    def query(self, q):
        rows, word_ids = self.get_match_rows(q)
        if word_ids == []:
            return '(0.0)Ops! Ur query is too difficult for me due to it is so rare...\n\nCould U speak English? :)'
        scores = self.get_scored_list(rows, word_ids)
        ranked_scores = sorted(
            [(score, url) for url, score in scores.items()], reverse=1)
        res = 'Those are ur query results :)\n-------------------------------\n'
        for i, (score, urlid) in enumerate(ranked_scores[0:10]):
            res += '%d. %s\n\n' % (i + 1, self.get_url_name(urlid))
        return res[:-2]

    def frequency_score(self, rows):  # 单词频度
        counts = dict([(row[0], 0) for row in rows])
        #         print(counts)s
        for row in rows:
            counts[row[0]] += 1
            return self.normalize_scores(counts, small_is_better=False)

    def location_score(self, rows):  # 单词位置
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc
        return self.normalize_scores(locations, small_is_better=True)

    def distance_score(self, rows):  # 单词距离
        # 如果仅有一个单词则得分一样
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])
        # 初始化字典，并且填入一个很大的数字
        min_distance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            sorted_row = sorted(row[1:])
            dist = sum([
                abs(sorted_row[i] - sorted_row[i - 1])
                for i in range(1, len(sorted_row))
            ])
            if dist < min_distance[row[0]]:
                min_distance[row[0]] = dist
        return self.normalize_scores(min_distance, small_is_better=True)

    def inbound_link_score(self, rows):  # 外部回指链接
        unique_urls = set([row[0] for row in rows])
        inbound_count = dict(
            [(u,
              self.cursor.execute("select count(*) from link where to_id=?",
                                  (u, )).fetchone()[0]) for u in unique_urls])
        return self.normalize_scores(inbound_count, small_is_better=False)

    # 归一化处理：使评价值介于0-1之间(1代表最佳结果)，因此能将不同方法返回的结果进行比较
    def normalize_scores(self, scores, small_is_better=False):
        vsmall = 0.00001  # 避免被零整除
        if small_is_better is True:
            min_score = float(min(scores.values()))
            return dict(
                [(u, min_score / max(vsmall, c)) for (u, c) in scores.items()])
        else:
            max_score = float(max(scores.values()))
            if max_score == 0.0:
                max_score = vsmall
            return dict([(u, c / max_score) for (u, c) in scores.items()])

    def calculate_pagerank(self, iterations=20):
        # 清除当前的pagerank
        self.con.execute('drop table if exists page_rank')
        self.con.execute(
            'create table page_rank(url_id primary key,score real not null)')

        # 初始化每一个url，令它等于1
        self.con.execute(
            'insert into page_rank select row_id,1.0 from url_list')
        self.dbcommit()

        for _ in range(iterations):
            #print("Iteration %d" % (i))
            for (url_id, ) in self.con.execute('select rowid from url_list'):
                pr = 0.15
                # 循环所有指向这个页面的外部链接 (循环找出每个指向该页面的url的分数)
                for (linker, ) in self.con.execute(
                        'select distinct from_id from link where to_id=%d' %
                        url_id):
                    linking_pr = self.con.execute(
                        'select score from page_rank where url_id=%d' %
                        linker).fetchone()[0]

                    # 根据链接源，求得总的连接数 (从该url发出的link总数)
                    linking_count = self.con.execute(
                        'select count(*) from link where from_id=%d' %
                        linker).fetchone()[0]
                    pr += 0.85 * (linking_pr / linking_count)
                self.con.execute(
                    'update page_rank set score=%f where url_id=%d' % (pr,
                                                                       url_id))
        self.dbcommit()

    def link_text_score(self, rows, word_ids):
        link_scores = dict([(row[0], 0) for row in rows])
        for word_id in word_ids:  #找出指向该URL的所有链接，并把它们的PR值加起来，越大越好
            cur = self.cursor.execute(
                'select link.from_id,link.to_id from link_words,link \
                 where word_id=? and link_words.link_id=link.id', (word_id, ))
            for (from_id, to_id) in cur:
                if to_id in link_scores:
                    pr = self.cursor.execute(
                        'select score from page_rank where url_id=?',
                        (from_id, )).fetchone()[0]
                    link_scores[to_id] += pr
        max_score = float(max(link_scores.values()))
        normalized_scores = dict(
            [(u, l / max_score) for (u, l) in link_scores.items()])
        return normalized_scores