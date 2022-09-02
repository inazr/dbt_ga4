{{
    config(
        materialized = 'incremental',
        partition_by = {
              "field": "session_reporting_date",
              "data_type": "date",
              "granularity": "day"
        },

        cluster_by = 'session_reporting_date'
    )

}}

WITH default_channel_grouping_cte AS (

    SELECT
            int_ga4__session_reporting_date.ga_session_id,
            int_ga4__session_reporting_date.user_pseudo_id,
            int_ga4__session_reporting_date.unique_session_id,
            int_ga4__session_reporting_date.{{ var('ga4__session_reporting_date') }} AS session_reporting_date,
            stg_ga4__flat_events.source,
            stg_ga4__flat_events.medium,
            stg_ga4__event_params.string_value AS campaign,
            CASE    WHEN stg_ga4__flat_events.source = '(direct)'
                     AND (stg_ga4__flat_events.medium = '(none)' OR stg_ga4__flat_events.medium = '(not set)')
                    THEN 'Direct'

                    WHEN stg_ga4__event_params.string_value LIKE '%cross-network%'
                    THEN 'Cross-network'

                    WHEN (stg_ga4__flat_events.source IN ('Google Shopping', 'IGShopping', 'aax-us-east.amazon-adsystem.com', 'aax.amazon-adsystem.com', 'alibaba', 'alibaba.com', 'amazon', 'amazon.co.uk', 'amazon.com', 'apps.shopify.com', 'checkout.shopify.com', 'checkout.stripe.com', 'cr.shopping.naver.com', 'cr2.shopping.naver.com', 'ebay', 'ebay.co.uk', 'ebay.com', 'ebay.com.au', 'ebay.de', 'etsy', 'etsy.com', 'm.alibaba.com', 'm.shopping.naver.com', 'mercadolibre', 'mercadolibre.com', 'mercadolibre.com.ar', 'mercadolibre.com.mx', 'message.alibaba.com', 'msearch.shopping.naver.com', 'nl.shopping.net', 'no.shopping.net', 'offer.alibaba.com', 'one.walmart.com', 'order.shopping.yahoo.co.jp', 'partners.shopify.com', 's3.amazonaws.com', 'se.shopping.net', 'shop.app', 'shopify', 'shopify.com', 'shopping.naver.com', 'shopping.yahoo.co.jp', 'shopping.yahoo.com', 'shopzilla', 'shopzilla.com', 'simplycodes.com', 'store.shopping.yahoo.co.jp', 'stripe', 'stripe.com', 'uk.shopping.net', 'walmart', 'walmart.com')
                      OR REGEXP_CONTAINS(stg_ga4__event_params.string_value, r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
                     AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                    THEN 'Paid Shopping'

                    WHEN stg_ga4__flat_events.source IN ('360.cn', 'alice', 'aol', 'ar.search.yahoo.com', 'ask', 'at.search.yahoo.com', 'au.search.yahoo.com', 'auone', 'avg', 'babylon', 'baidu', 'biglobe', 'biglobe.co.jp', 'biglobe.ne.jp', 'bing', 'br.search.yahoo.com', 'ca.search.yahoo.com', 'centrum.cz', 'ch.search.yahoo.com', 'cl.search.yahoo.com', 'cn.bing.com', 'cnn', 'co.search.yahoo.com', 'comcast', 'conduit', 'cse.google.com', 'daum', 'daum.net', 'de.search.yahoo.com', 'dk.search.yahoo.com', 'dogpile', 'dogpile.com', 'duckduckgo', 'ecosia.org', 'email.seznam.cz', 'eniro', 'es.search.yahoo.com', 'espanol.search.yahoo.com', 'exalead.com', 'excite.com', 'fi.search.yahoo.com', 'firmy.cz', 'fr.search.yahoo.com', 'globo', 'go.mail.ru', 'google', 'google-play', 'google.com', 'googlemybusiness', 'hk.search.yahoo.com', 'id.search.yahoo.com', 'in.search.yahoo.com', 'incredimail', 'it.search.yahoo.com', 'kvasir', 'lite.qwant.com', 'lycos', 'm.baidu.com', 'm.naver.com', 'm.search.naver.com', 'm.sogou.com', 'mail.google.com', 'mail.rambler.ru', 'mail.yandex.ru', 'malaysia.search.yahoo.com', 'msn', 'msn.com', 'mx.search.yahoo.com', 'najdi', 'naver', 'naver.com', 'news.google.com', 'nl.search.yahoo.com', 'no.search.yahoo.com', 'ntp.msn.com', 'nz.search.yahoo.com', 'onet', 'onet.pl', 'pe.search.yahoo.com', 'ph.search.yahoo.com', 'pl.search.yahoo.com', 'qwant', 'qwant.com', 'rakuten', 'rakuten.co.jp', 'rambler', 'rambler.ru', 'se.search.yahoo.com', 'search-results', 'search.aol.co.uk', 'search.aol.com', 'search.google.com', 'search.smt.docomo.ne.jp', 'search.ukr.net', 'secureurl.ukr.net', 'seznam', 'seznam.cz', 'sg.search.yahoo.com', 'so.com', 'sogou', 'sogou.com', 'sp-web.search.auone.jp', 'startsiden', 'startsiden.no', 'suche.aol.de', 'terra', 'th.search.yahoo.com', 'tr.search.yahoo.com', 'tut.by', 'tw.search.yahoo.com', 'uk.search.yahoo.com', 'ukr', 'us.search.yahoo.com', 'virgilio', 'vn.search.yahoo.com', 'wap.sogou.com', 'webmaster.yandex.ru', 'websearch.rakuten.co.jp', 'yahoo', 'yahoo.co.jp', 'yahoo.com', 'yandex', 'yandex.by', 'yandex.com', 'yandex.com.tr', 'yandex.fr', 'yandex.kz', 'yandex.ru', 'yandex.ua', 'yandex.uz', 'zen.yandex.ru')
                     AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                    THEN 'Paid Search'

                    WHEN stg_ga4__flat_events.source IN ('43things', '43things.com', '51.com', '5ch.net', 'Hatena', 'ImageShack', 'academia.edu', 'activerain', 'activerain.com', 'activeworlds', 'activeworlds.com', 'addthis', 'addthis.com', 'airg.ca', 'allnurses.com', 'allrecipes.com', 'alumniclass', 'alumniclass.com', 'ameba.jp', 'ameblo.jp', 'americantowns', 'americantowns.com', 'amp.reddit.com', 'ancestry.com', 'anobii', 'anobii.com', 'answerbag', 'answerbag.com', 'answers.yahoo.com', 'aolanswers', 'aolanswers.com', 'apps.facebook.com', 'ar.pinterest.com', 'artstation.com', 'askubuntu', 'askubuntu.com', 'asmallworld.com', 'athlinks', 'athlinks.com', 'away.vk.com', 'awe.sm', 'b.hatena.ne.jp', 'baby-gaga', 'baby-gaga.com', 'babyblog.ru', 'badoo', 'badoo.com', 'bebo', 'bebo.com', 'beforeitsnews', 'beforeitsnews.com', 'bharatstudent', 'bharatstudent.com', 'biip.no', 'biswap.org', 'bit.ly', 'blackcareernetwork.com', 'blackplanet', 'blackplanet.com', 'blip.fm', 'blog.com', 'blog.feedspot.com', 'blog.goo.ne.jp', 'blog.naver.com', 'blog.yahoo.co.jp', 'blogg.no', 'bloggang.com', 'blogger', 'blogger.com', 'blogher', 'blogher.com', 'bloglines', 'bloglines.com', 'blogs.com', 'blogsome', 'blogsome.com', 'blogspot', 'blogspot.com', 'blogster', 'blogster.com', 'blurtit', 'blurtit.com', 'bookmarks.yahoo.co.jp', 'bookmarks.yahoo.com', 'br.pinterest.com', 'brightkite', 'brightkite.com', 'brizzly', 'brizzly.com', 'business.facebook.com', 'buzzfeed', 'buzzfeed.com', 'buzznet', 'buzznet.com', 'cafe.naver.com', 'cafemom', 'cafemom.com', 'camospace', 'camospace.com', 'canalblog.com', 'care.com', 'care2', 'care2.com', 'caringbridge.org', 'catster', 'catster.com', 'cbnt.io', 'cellufun', 'cellufun.com', 'centerblog.net', 'chat.zalo.me', 'chegg.com', 'chicagonow', 'chicagonow.com', 'chiebukuro.yahoo.co.jp', 'classmates', 'classmates.com', 'classquest', 'classquest.com', 'co.pinterest.com', 'cocolog-nifty', 'cocolog-nifty.com', 'copainsdavant.linternaute.com', 'couchsurfing.org', 'cozycot', 'cozycot.com', 'cross.tv', 'crunchyroll', 'crunchyroll.com', 'cyworld', 'cyworld.com', 'cz.pinterest.com', 'd.hatena.ne.jp', 'dailystrength.org', 'deluxe.com', 'deviantart', 'deviantart.com', 'dianping', 'dianping.com', 'digg', 'digg.com', 'diigo', 'diigo.com', 'discover.hubpages.com', 'disqus', 'disqus.com', 'dogster', 'dogster.com', 'dol2day', 'dol2day.com', 'doostang', 'doostang.com', 'dopplr', 'dopplr.com', 'douban', 'douban.com', 'draft.blogger.com', 'draugiem.lv', 'drugs-forum', 'drugs-forum.com', 'dzone', 'dzone.com', 'edublogs.org', 'elftown', 'elftown.com', 'epicurious.com', 'everforo.com', 'exblog.jp', 'extole', 'extole.com', 'facebook', 'facebook.com', 'faceparty', 'faceparty.com', 'fandom.com', 'fanpop', 'fanpop.com', 'fark', 'fark.com', 'fb', 'fb.me', 'fc2', 'fc2.com', 'feedspot', 'feministing', 'feministing.com', 'filmaffinity', 'filmaffinity.com', 'flickr', 'flickr.com', 'flipboard', 'flipboard.com', 'folkdirect', 'folkdirect.com', 'foodservice', 'foodservice.com', 'forums.androidcentral.com', 'forums.crackberry.com', 'forums.imore.com', 'forums.nexopia.com', 'forums.webosnation.com', 'forums.wpcentral.com', 'fotki', 'fotki.com', 'fotolog', 'fotolog.com', 'foursquare', 'foursquare.com', 'free.facebook.com', 'friendfeed', 'friendfeed.com', 'fruehstueckstreff.org', 'fubar', 'fubar.com', 'gaiaonline', 'gaiaonline.com', 'gamerdna', 'gamerdna.com', 'gather.com', 'geni.com', 'getpocket.com', 'glassboard', 'glassboard.com', 'glassdoor', 'glassdoor.com', 'godtube', 'godtube.com', 'goldenline.pl', 'goldstar', 'goldstar.com', 'goo.gl', 'gooblog', 'goodreads', 'goodreads.com', 'google+', 'googlegroups.com', 'googleplus', 'govloop', 'govloop.com', 'gowalla', 'gowalla.com', 'gree.jp', 'groups.google.com', 'gulli.com', 'gutefrage.net', 'habbo', 'habbo.com', 'hi5', 'hi5.com', 'hootsuite', 'hootsuite.com', 'houzz', 'houzz.com', 'hoverspot', 'hoverspot.com', 'hr.com', 'hu.pinterest.com', 'hubculture', 'hubculture.com', 'hubpages.com', 'hyves.net', 'hyves.nl', 'ibibo', 'ibibo.com', 'id.pinterest.com', 'identi.ca', 'ig', 'imageshack.com', 'imageshack.us', 'imvu', 'imvu.com', 'in.pinterest.com', 'insanejournal', 'insanejournal.com', 'instagram', 'instagram.com', 'instapaper', 'instapaper.com', 'internations.org', 'interpals.net', 'intherooms', 'intherooms.com', 'irc-galleria.net', 'is.gd', 'italki', 'italki.com', 'jammerdirect', 'jammerdirect.com', 'jappy.com', 'jappy.de', 'kaboodle.com', 'kakao', 'kakao.com', 'kakaocorp.com', 'kaneva', 'kaneva.com', 'kin.naver.com', 'l.facebook.com', 'l.instagram.com', 'l.messenger.com', 'last.fm', 'librarything', 'librarything.com', 'lifestream.aol.com', 'line', 'line.me', 'linkedin', 'linkedin.com', 'listal', 'listal.com', 'listography', 'listography.com', 'livedoor.com', 'livedoorblog', 'livejournal', 'livejournal.com', 'lm.facebook.com', 'lnkd.in', 'm.blog.naver.com', 'm.cafe.naver.com', 'm.facebook.com', 'm.kin.naver.com', 'm.vk.com', 'm.yelp.com', 'mbga.jp', 'medium.com', 'meetin.org', 'meetup', 'meetup.com', 'meinvz.net', 'meneame.net', 'menuism.com', 'messages.google.com', 'messages.yahoo.co.jp', 'messenger', 'messenger.com', 'mix.com', 'mixi.jp', 'mobile.facebook.com', 'mocospace', 'mocospace.com', 'mouthshut', 'mouthshut.com', 'movabletype', 'movabletype.com', 'mubi', 'mubi.com', 'my.opera.com', 'myanimelist.net', 'myheritage', 'myheritage.com', 'mylife', 'mylife.com', 'mymodernmet', 'mymodernmet.com', 'myspace', 'myspace.com', 'netvibes', 'netvibes.com', 'news.ycombinator.com', 'newsshowcase', 'nexopia', 'ngopost.org', 'niconico', 'nicovideo.jp', 'nightlifelink', 'nightlifelink.com', 'ning', 'ning.com', 'nl.pinterest.com', 'odnoklassniki.ru', 'odnoklassniki.ua', 'okwave.jp', 'old.reddit.com', 'oneworldgroup.org', 'onstartups', 'onstartups.com', 'opendiary', 'opendiary.com', 'oshiete.goo.ne.jp', 'out.reddit.com', 'over-blog.com', 'overblog.com', 'paper.li', 'partyflock.nl', 'photobucket', 'photobucket.com', 'pinboard', 'pinboard.in', 'pingsta', 'pingsta.com', 'pinterest', 'pinterest.at', 'pinterest.ca', 'pinterest.ch', 'pinterest.cl', 'pinterest.co.kr', 'pinterest.co.uk', 'pinterest.com', 'pinterest.com.au', 'pinterest.com.mx', 'pinterest.de', 'pinterest.es', 'pinterest.fr', 'pinterest.it', 'pinterest.jp', 'pinterest.nz', 'pinterest.ph', 'pinterest.pt', 'pinterest.ru', 'pinterest.se', 'pixiv.net', 'pl.pinterest.com', 'playahead.se', 'plurk', 'plurk.com', 'plus.google.com', 'plus.url.google.com', 'pocket.co', 'posterous', 'posterous.com', 'pro.homeadvisor.com', 'pulse.yahoo.com', 'qapacity', 'qapacity.com', 'quechup', 'quechup.com', 'quora', 'quora.com', 'qzone.qq.com', 'ravelry', 'ravelry.com', 'reddit', 'reddit.com', 'redux', 'redux.com', 'renren', 'renren.com', 'researchgate.net', 'reunion', 'reunion.com', 'reverbnation', 'reverbnation.com', 'rtl.de', 'ryze', 'ryze.com', 'salespider', 'salespider.com', 'scoop.it', 'screenrant', 'screenrant.com', 'scribd', 'scribd.com', 'scvngr', 'scvngr.com', 'secondlife', 'secondlife.com', 'serverfault', 'serverfault.com', 'shareit', 'sharethis', 'sharethis.com', 'shvoong.com', 'sites.google.com', 'skype', 'skyrock', 'skyrock.com', 'slashdot.org', 'slideshare.net', 'smartnews.com', 'snapchat', 'snapchat.com', 'sociallife.com.br', 'socialvibe', 'socialvibe.com', 'spaces.live.com', 'spoke', 'spoke.com', 'spruz', 'spruz.com', 'ssense.com', 'stackapps', 'stackapps.com', 'stackexchange', 'stackexchange.com', 'stackoverflow', 'stackoverflow.com', 'stardoll.com', 'stickam', 'stickam.com', 'studivz.net', 'suomi24.fi', 'superuser', 'superuser.com', 'sweeva', 'sweeva.com', 't.co', 't.me', 'tagged', 'tagged.com', 'taggedmail', 'taggedmail.com', 'talkbiznow', 'talkbiznow.com', 'taringa.net', 'techmeme', 'techmeme.com', 'tencent', 'tencent.com', 'tiktok', 'tiktok.com', 'tinyurl', 'tinyurl.com', 'toolbox', 'toolbox.com', 'touch.facebook.com', 'tr.pinterest.com', 'travellerspoint', 'travellerspoint.com', 'tripadvisor', 'tripadvisor.com', 'trombi', 'trombi.com', 'tudou', 'tudou.com', 'tuenti', 'tuenti.com', 'tumblr', 'tumblr.com', 'tweetdeck', 'tweetdeck.com', 'twitter', 'twitter.com', 'twoo.com', 'typepad', 'typepad.com', 'unblog.fr', 'urbanspoon.com', 'ushareit.com', 'ushi.cn', 'vampirefreaks', 'vampirefreaks.com', 'vampirerave', 'vampirerave.com', 'vg.no', 'video.ibm.com', 'vk.com', 'vkontakte.ru', 'wakoopa', 'wakoopa.com', 'wattpad', 'wattpad.com', 'web.facebook.com', 'web.skype.com', 'webshots', 'webshots.com', 'wechat', 'wechat.com', 'weebly', 'weebly.com', 'weibo', 'weibo.com', 'wer-weiss-was.de', 'weread', 'weread.com', 'whatsapp', 'whatsapp.com', 'wiki.answers.com', 'wikihow.com', 'wikitravel.org', 'woot.com', 'wordpress', 'wordpress.com', 'wordpress.org', 'xanga', 'xanga.com', 'xing', 'xing.com', 'yahoo-mbga.jp', 'yammer', 'yammer.com', 'yelp', 'yelp.co.uk', 'yelp.com', 'youroom.in', 'za.pinterest.com', 'zalo', 'zoo.gr', 'zooppa', 'zooppa.com')
                     AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                    THEN 'Paid Social'

                    WHEN stg_ga4__flat_events.source IN ('blog.twitch.tv', 'crackle', 'crackle.com', 'curiositystream', 'curiositystream.com', 'd.tube', 'dailymotion', 'dailymotion.com', 'dashboard.twitch.tv', 'disneyplus', 'disneyplus.com', 'fast.wistia.net', 'help.hulu.com', 'help.netflix.com', 'hulu', 'hulu.com', 'id.twitch.tv', 'iq.com', 'iqiyi', 'iqiyi.com', 'jobs.netflix.com', 'justin.tv', 'm.twitch.tv', 'm.youtube.com', 'music.youtube.com', 'netflix', 'netflix.com', 'player.twitch.tv', 'player.vimeo.com', 'ted', 'ted.com', 'twitch', 'twitch.tv', 'utreon', 'utreon.com', 'veoh', 'veoh.com', 'viadeo.journaldunet.com', 'vimeo', 'vimeo.com', 'wistia', 'wistia.com', 'youku', 'youku.com', 'youtube', 'youtube.com')
                     AND REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*cp.*|ppc|paid.*)$')
                    THEN 'Paid Video'

                    WHEN stg_ga4__flat_events.medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm')
                    THEN 'Display'

                     WHEN (stg_ga4__flat_events.source IN ('Google Shopping', 'IGShopping', 'aax-us-east.amazon-adsystem.com', 'aax.amazon-adsystem.com', 'alibaba', 'alibaba.com', 'amazon', 'amazon.co.uk', 'amazon.com', 'apps.shopify.com', 'checkout.shopify.com', 'checkout.stripe.com', 'cr.shopping.naver.com', 'cr2.shopping.naver.com', 'ebay', 'ebay.co.uk', 'ebay.com', 'ebay.com.au', 'ebay.de', 'etsy', 'etsy.com', 'm.alibaba.com', 'm.shopping.naver.com', 'mercadolibre', 'mercadolibre.com', 'mercadolibre.com.ar', 'mercadolibre.com.mx', 'message.alibaba.com', 'msearch.shopping.naver.com', 'nl.shopping.net', 'no.shopping.net', 'offer.alibaba.com', 'one.walmart.com', 'order.shopping.yahoo.co.jp', 'partners.shopify.com', 's3.amazonaws.com', 'se.shopping.net', 'shop.app', 'shopify', 'shopify.com', 'shopping.naver.com', 'shopping.yahoo.co.jp', 'shopping.yahoo.com', 'shopzilla', 'shopzilla.com', 'simplycodes.com', 'store.shopping.yahoo.co.jp', 'stripe', 'stripe.com', 'uk.shopping.net', 'walmart', 'walmart.com')
                       OR REGEXP_CONTAINS( stg_ga4__event_params.string_value, r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
                     THEN 'Organic Shopping'

                    WHEN stg_ga4__flat_events.source IN ('43things', '43things.com', '51.com', '5ch.net', 'Hatena', 'ImageShack', 'academia.edu', 'activerain', 'activerain.com', 'activeworlds', 'activeworlds.com', 'addthis', 'addthis.com', 'airg.ca', 'allnurses.com', 'allrecipes.com', 'alumniclass', 'alumniclass.com', 'ameba.jp', 'ameblo.jp', 'americantowns', 'americantowns.com', 'amp.reddit.com', 'ancestry.com', 'anobii', 'anobii.com', 'answerbag', 'answerbag.com', 'answers.yahoo.com', 'aolanswers', 'aolanswers.com', 'apps.facebook.com', 'ar.pinterest.com', 'artstation.com', 'askubuntu', 'askubuntu.com', 'asmallworld.com', 'athlinks', 'athlinks.com', 'away.vk.com', 'awe.sm', 'b.hatena.ne.jp', 'baby-gaga', 'baby-gaga.com', 'babyblog.ru', 'badoo', 'badoo.com', 'bebo', 'bebo.com', 'beforeitsnews', 'beforeitsnews.com', 'bharatstudent', 'bharatstudent.com', 'biip.no', 'biswap.org', 'bit.ly', 'blackcareernetwork.com', 'blackplanet', 'blackplanet.com', 'blip.fm', 'blog.com', 'blog.feedspot.com', 'blog.goo.ne.jp', 'blog.naver.com', 'blog.yahoo.co.jp', 'blogg.no', 'bloggang.com', 'blogger', 'blogger.com', 'blogher', 'blogher.com', 'bloglines', 'bloglines.com', 'blogs.com', 'blogsome', 'blogsome.com', 'blogspot', 'blogspot.com', 'blogster', 'blogster.com', 'blurtit', 'blurtit.com', 'bookmarks.yahoo.co.jp', 'bookmarks.yahoo.com', 'br.pinterest.com', 'brightkite', 'brightkite.com', 'brizzly', 'brizzly.com', 'business.facebook.com', 'buzzfeed', 'buzzfeed.com', 'buzznet', 'buzznet.com', 'cafe.naver.com', 'cafemom', 'cafemom.com', 'camospace', 'camospace.com', 'canalblog.com', 'care.com', 'care2', 'care2.com', 'caringbridge.org', 'catster', 'catster.com', 'cbnt.io', 'cellufun', 'cellufun.com', 'centerblog.net', 'chat.zalo.me', 'chegg.com', 'chicagonow', 'chicagonow.com', 'chiebukuro.yahoo.co.jp', 'classmates', 'classmates.com', 'classquest', 'classquest.com', 'co.pinterest.com', 'cocolog-nifty', 'cocolog-nifty.com', 'copainsdavant.linternaute.com', 'couchsurfing.org', 'cozycot', 'cozycot.com', 'cross.tv', 'crunchyroll', 'crunchyroll.com', 'cyworld', 'cyworld.com', 'cz.pinterest.com', 'd.hatena.ne.jp', 'dailystrength.org', 'deluxe.com', 'deviantart', 'deviantart.com', 'dianping', 'dianping.com', 'digg', 'digg.com', 'diigo', 'diigo.com', 'discover.hubpages.com', 'disqus', 'disqus.com', 'dogster', 'dogster.com', 'dol2day', 'dol2day.com', 'doostang', 'doostang.com', 'dopplr', 'dopplr.com', 'douban', 'douban.com', 'draft.blogger.com', 'draugiem.lv', 'drugs-forum', 'drugs-forum.com', 'dzone', 'dzone.com', 'edublogs.org', 'elftown', 'elftown.com', 'epicurious.com', 'everforo.com', 'exblog.jp', 'extole', 'extole.com', 'facebook', 'facebook.com', 'faceparty', 'faceparty.com', 'fandom.com', 'fanpop', 'fanpop.com', 'fark', 'fark.com', 'fb', 'fb.me', 'fc2', 'fc2.com', 'feedspot', 'feministing', 'feministing.com', 'filmaffinity', 'filmaffinity.com', 'flickr', 'flickr.com', 'flipboard', 'flipboard.com', 'folkdirect', 'folkdirect.com', 'foodservice', 'foodservice.com', 'forums.androidcentral.com', 'forums.crackberry.com', 'forums.imore.com', 'forums.nexopia.com', 'forums.webosnation.com', 'forums.wpcentral.com', 'fotki', 'fotki.com', 'fotolog', 'fotolog.com', 'foursquare', 'foursquare.com', 'free.facebook.com', 'friendfeed', 'friendfeed.com', 'fruehstueckstreff.org', 'fubar', 'fubar.com', 'gaiaonline', 'gaiaonline.com', 'gamerdna', 'gamerdna.com', 'gather.com', 'geni.com', 'getpocket.com', 'glassboard', 'glassboard.com', 'glassdoor', 'glassdoor.com', 'godtube', 'godtube.com', 'goldenline.pl', 'goldstar', 'goldstar.com', 'goo.gl', 'gooblog', 'goodreads', 'goodreads.com', 'google+', 'googlegroups.com', 'googleplus', 'govloop', 'govloop.com', 'gowalla', 'gowalla.com', 'gree.jp', 'groups.google.com', 'gulli.com', 'gutefrage.net', 'habbo', 'habbo.com', 'hi5', 'hi5.com', 'hootsuite', 'hootsuite.com', 'houzz', 'houzz.com', 'hoverspot', 'hoverspot.com', 'hr.com', 'hu.pinterest.com', 'hubculture', 'hubculture.com', 'hubpages.com', 'hyves.net', 'hyves.nl', 'ibibo', 'ibibo.com', 'id.pinterest.com', 'identi.ca', 'ig', 'imageshack.com', 'imageshack.us', 'imvu', 'imvu.com', 'in.pinterest.com', 'insanejournal', 'insanejournal.com', 'instagram', 'instagram.com', 'instapaper', 'instapaper.com', 'internations.org', 'interpals.net', 'intherooms', 'intherooms.com', 'irc-galleria.net', 'is.gd', 'italki', 'italki.com', 'jammerdirect', 'jammerdirect.com', 'jappy.com', 'jappy.de', 'kaboodle.com', 'kakao', 'kakao.com', 'kakaocorp.com', 'kaneva', 'kaneva.com', 'kin.naver.com', 'l.facebook.com', 'l.instagram.com', 'l.messenger.com', 'last.fm', 'librarything', 'librarything.com', 'lifestream.aol.com', 'line', 'line.me', 'linkedin', 'linkedin.com', 'listal', 'listal.com', 'listography', 'listography.com', 'livedoor.com', 'livedoorblog', 'livejournal', 'livejournal.com', 'lm.facebook.com', 'lnkd.in', 'm.blog.naver.com', 'm.cafe.naver.com', 'm.facebook.com', 'm.kin.naver.com', 'm.vk.com', 'm.yelp.com', 'mbga.jp', 'medium.com', 'meetin.org', 'meetup', 'meetup.com', 'meinvz.net', 'meneame.net', 'menuism.com', 'messages.google.com', 'messages.yahoo.co.jp', 'messenger', 'messenger.com', 'mix.com', 'mixi.jp', 'mobile.facebook.com', 'mocospace', 'mocospace.com', 'mouthshut', 'mouthshut.com', 'movabletype', 'movabletype.com', 'mubi', 'mubi.com', 'my.opera.com', 'myanimelist.net', 'myheritage', 'myheritage.com', 'mylife', 'mylife.com', 'mymodernmet', 'mymodernmet.com', 'myspace', 'myspace.com', 'netvibes', 'netvibes.com', 'news.ycombinator.com', 'newsshowcase', 'nexopia', 'ngopost.org', 'niconico', 'nicovideo.jp', 'nightlifelink', 'nightlifelink.com', 'ning', 'ning.com', 'nl.pinterest.com', 'odnoklassniki.ru', 'odnoklassniki.ua', 'okwave.jp', 'old.reddit.com', 'oneworldgroup.org', 'onstartups', 'onstartups.com', 'opendiary', 'opendiary.com', 'oshiete.goo.ne.jp', 'out.reddit.com', 'over-blog.com', 'overblog.com', 'paper.li', 'partyflock.nl', 'photobucket', 'photobucket.com', 'pinboard', 'pinboard.in', 'pingsta', 'pingsta.com', 'pinterest', 'pinterest.at', 'pinterest.ca', 'pinterest.ch', 'pinterest.cl', 'pinterest.co.kr', 'pinterest.co.uk', 'pinterest.com', 'pinterest.com.au', 'pinterest.com.mx', 'pinterest.de', 'pinterest.es', 'pinterest.fr', 'pinterest.it', 'pinterest.jp', 'pinterest.nz', 'pinterest.ph', 'pinterest.pt', 'pinterest.ru', 'pinterest.se', 'pixiv.net', 'pl.pinterest.com', 'playahead.se', 'plurk', 'plurk.com', 'plus.google.com', 'plus.url.google.com', 'pocket.co', 'posterous', 'posterous.com', 'pro.homeadvisor.com', 'pulse.yahoo.com', 'qapacity', 'qapacity.com', 'quechup', 'quechup.com', 'quora', 'quora.com', 'qzone.qq.com', 'ravelry', 'ravelry.com', 'reddit', 'reddit.com', 'redux', 'redux.com', 'renren', 'renren.com', 'researchgate.net', 'reunion', 'reunion.com', 'reverbnation', 'reverbnation.com', 'rtl.de', 'ryze', 'ryze.com', 'salespider', 'salespider.com', 'scoop.it', 'screenrant', 'screenrant.com', 'scribd', 'scribd.com', 'scvngr', 'scvngr.com', 'secondlife', 'secondlife.com', 'serverfault', 'serverfault.com', 'shareit', 'sharethis', 'sharethis.com', 'shvoong.com', 'sites.google.com', 'skype', 'skyrock', 'skyrock.com', 'slashdot.org', 'slideshare.net', 'smartnews.com', 'snapchat', 'snapchat.com', 'sociallife.com.br', 'socialvibe', 'socialvibe.com', 'spaces.live.com', 'spoke', 'spoke.com', 'spruz', 'spruz.com', 'ssense.com', 'stackapps', 'stackapps.com', 'stackexchange', 'stackexchange.com', 'stackoverflow', 'stackoverflow.com', 'stardoll.com', 'stickam', 'stickam.com', 'studivz.net', 'suomi24.fi', 'superuser', 'superuser.com', 'sweeva', 'sweeva.com', 't.co', 't.me', 'tagged', 'tagged.com', 'taggedmail', 'taggedmail.com', 'talkbiznow', 'talkbiznow.com', 'taringa.net', 'techmeme', 'techmeme.com', 'tencent', 'tencent.com', 'tiktok', 'tiktok.com', 'tinyurl', 'tinyurl.com', 'toolbox', 'toolbox.com', 'touch.facebook.com', 'tr.pinterest.com', 'travellerspoint', 'travellerspoint.com', 'tripadvisor', 'tripadvisor.com', 'trombi', 'trombi.com', 'tudou', 'tudou.com', 'tuenti', 'tuenti.com', 'tumblr', 'tumblr.com', 'tweetdeck', 'tweetdeck.com', 'twitter', 'twitter.com', 'twoo.com', 'typepad', 'typepad.com', 'unblog.fr', 'urbanspoon.com', 'ushareit.com', 'ushi.cn', 'vampirefreaks', 'vampirefreaks.com', 'vampirerave', 'vampirerave.com', 'vg.no', 'video.ibm.com', 'vk.com', 'vkontakte.ru', 'wakoopa', 'wakoopa.com', 'wattpad', 'wattpad.com', 'web.facebook.com', 'web.skype.com', 'webshots', 'webshots.com', 'wechat', 'wechat.com', 'weebly', 'weebly.com', 'weibo', 'weibo.com', 'wer-weiss-was.de', 'weread', 'weread.com', 'whatsapp', 'whatsapp.com', 'wiki.answers.com', 'wikihow.com', 'wikitravel.org', 'woot.com', 'wordpress', 'wordpress.com', 'wordpress.org', 'xanga', 'xanga.com', 'xing', 'xing.com', 'yahoo-mbga.jp', 'yammer', 'yammer.com', 'yelp', 'yelp.co.uk', 'yelp.com', 'youroom.in', 'za.pinterest.com', 'zalo', 'zoo.gr', 'zooppa', 'zooppa.com')
                      OR stg_ga4__flat_events.medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media')
                    THEN 'Organic Social'

                    WHEN stg_ga4__flat_events.source IN ('blog.twitch.tv', 'crackle', 'crackle.com', 'curiositystream', 'curiositystream.com', 'd.tube', 'dailymotion', 'dailymotion.com', 'dashboard.twitch.tv', 'disneyplus', 'disneyplus.com', 'fast.wistia.net', 'help.hulu.com', 'help.netflix.com', 'hulu', 'hulu.com', 'id.twitch.tv', 'iq.com', 'iqiyi', 'iqiyi.com', 'jobs.netflix.com', 'justin.tv', 'm.twitch.tv', 'm.youtube.com', 'music.youtube.com', 'netflix', 'netflix.com', 'player.twitch.tv', 'player.vimeo.com', 'ted', 'ted.com', 'twitch', 'twitch.tv', 'utreon', 'utreon.com', 'veoh', 'veoh.com', 'viadeo.journaldunet.com', 'vimeo', 'vimeo.com', 'wistia', 'wistia.com', 'youku', 'youku.com', 'youtube', 'youtube.com')
                      OR REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*video.*)$')
                    THEN 'Organic Video'

                    WHEN stg_ga4__flat_events.source IN ('360.cn', 'alice', 'aol', 'ar.search.yahoo.com', 'ask', 'at.search.yahoo.com', 'au.search.yahoo.com', 'auone', 'avg', 'babylon', 'baidu', 'biglobe', 'biglobe.co.jp', 'biglobe.ne.jp', 'bing', 'br.search.yahoo.com', 'ca.search.yahoo.com', 'centrum.cz', 'ch.search.yahoo.com', 'cl.search.yahoo.com', 'cn.bing.com', 'cnn', 'co.search.yahoo.com', 'comcast', 'conduit', 'cse.google.com', 'daum', 'daum.net', 'de.search.yahoo.com', 'dk.search.yahoo.com', 'dogpile', 'dogpile.com', 'duckduckgo', 'ecosia.org', 'email.seznam.cz', 'eniro', 'es.search.yahoo.com', 'espanol.search.yahoo.com', 'exalead.com', 'excite.com', 'fi.search.yahoo.com', 'firmy.cz', 'fr.search.yahoo.com', 'globo', 'go.mail.ru', 'google', 'google-play', 'google.com', 'googlemybusiness', 'hk.search.yahoo.com', 'id.search.yahoo.com', 'in.search.yahoo.com', 'incredimail', 'it.search.yahoo.com', 'kvasir', 'lite.qwant.com', 'lycos', 'm.baidu.com', 'm.naver.com', 'm.search.naver.com', 'm.sogou.com', 'mail.google.com', 'mail.rambler.ru', 'mail.yandex.ru', 'malaysia.search.yahoo.com', 'msn', 'msn.com', 'mx.search.yahoo.com', 'najdi', 'naver', 'naver.com', 'news.google.com', 'nl.search.yahoo.com', 'no.search.yahoo.com', 'ntp.msn.com', 'nz.search.yahoo.com', 'onet', 'onet.pl', 'pe.search.yahoo.com', 'ph.search.yahoo.com', 'pl.search.yahoo.com', 'qwant', 'qwant.com', 'rakuten', 'rakuten.co.jp', 'rambler', 'rambler.ru', 'se.search.yahoo.com', 'search-results', 'search.aol.co.uk', 'search.aol.com', 'search.google.com', 'search.smt.docomo.ne.jp', 'search.ukr.net', 'secureurl.ukr.net', 'seznam', 'seznam.cz', 'sg.search.yahoo.com', 'so.com', 'sogou', 'sogou.com', 'sp-web.search.auone.jp', 'startsiden', 'startsiden.no', 'suche.aol.de', 'terra', 'th.search.yahoo.com', 'tr.search.yahoo.com', 'tut.by', 'tw.search.yahoo.com', 'uk.search.yahoo.com', 'ukr', 'us.search.yahoo.com', 'virgilio', 'vn.search.yahoo.com', 'wap.sogou.com', 'webmaster.yandex.ru', 'websearch.rakuten.co.jp', 'yahoo', 'yahoo.co.jp', 'yahoo.com', 'yandex', 'yandex.by', 'yandex.com', 'yandex.com.tr', 'yandex.fr', 'yandex.kz', 'yandex.ru', 'yandex.ua', 'yandex.uz', 'zen.yandex.ru')
                     AND stg_ga4__flat_events.medium = 'organic'
                    THEN 'Organic Search'

                    WHEN REGEXP_CONTAINS(stg_ga4__flat_events.source, r'^(.*e.mail.*)$')
                      OR REGEXP_CONTAINS(stg_ga4__flat_events.medium, r'^(.*e.mail.*)$')
                    THEN 'Email'

                    WHEN stg_ga4__flat_events.medium = 'affiliate'
                    THEN 'Affiliates'

                    WHEN stg_ga4__flat_events.medium = 'referral'
                    THEN 'Referral'

                    WHEN stg_ga4__flat_events.medium = 'audio'
                    THEN 'Audio'

                    WHEN stg_ga4__flat_events.medium = 'sms'
                    THEN 'SMS'

                    WHEN stg_ga4__flat_events.medium LIKE '%push'
                      OR stg_ga4__flat_events.medium LIKE '%mobile%'
                      OR stg_ga4__flat_events.medium LIKE '%notification%'
                    THEN 'Mobile Push Notifications'

            END AS default_channel_grouping,
            ROW_NUMBER() OVER (PARTITION BY int_ga4__session_reporting_date.unique_session_id ORDER BY MIN(stg_ga4__event_params.event_timestamp)) AS row_number_result
    FROM
            {{ ref('stg_ga4__flat_events') }}

    LEFT JOIN
            {{ ref('stg_ga4__event_params') }}
            ON stg_ga4__flat_events.unique_session_id = stg_ga4__event_params.unique_session_id
            AND stg_ga4__event_params.key = 'campaign'
            AND stg_ga4__event_params.event_name = 'page_view'

    LEFT JOIN
            {{ ref('int_ga4__session_reporting_date') }}
            ON stg_ga4__flat_events.unique_session_id = int_ga4__session_reporting_date.unique_session_id

    WHERE   1=1

            {% if is_incremental() %}

      AND   stg_ga4__flat_events.event_date >= (SELECT MAX(session_reporting_date) FROM {{ this }})

            {% endif %}

    GROUP BY
            1,2,3,4,5,6,7,8

)

SELECT
        * EXCEPT(row_number_result)
FROM
        default_channel_grouping_cte
WHERE
        row_number_result = 1
