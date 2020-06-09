package com.hupu.hermes.sink;

public class UserTrack {

    public long getDur() {
        return dur;
    }

    public void setDur(long dur) {
        this.dur = dur;
    }

    public long getEt() {
        return et;
    }

    public void setEt(long et) {
        this.et = et;
    }

    public String getAct() {
        return act;
    }

    public void setAct(String act) {
        this.act = act;
    }

    private long server_time = System.currentTimeMillis();
    private long et= System.currentTimeMillis();
    private long lt = System.currentTimeMillis();
    private long eid = System.currentTimeMillis();
    private long vt = System.currentTimeMillis();
    private long dur;




    private String uid = "33875349";
    private String clt = "fad5b052e2c9f9e9ad01ece53908991dd31aeb25";

    private String itemid = "match_52795354";
    private String su = "";
    private String sc = "S01.PISC0002.BMC001.T1";
    private String sty = "app";
    private String ip = "112.96.119.185";
    private String kv = "v1";


    private String tz = "Asia/Shanghai";
    private String api = "0";
    private String os = "Android";
    private String isp = "中国移动";
    private String mfrs = "iphone";
    private String model = "iphone x";
    private String mac = "48:FD:A3:F2:E6:A2";
    private String idfa = "5FF83F5D-D182-4A1B-A4E4-EA0FF3FCD47A";
    private String imeis = "869832045555071";
    private String andid = "85026e6de91fbf0c";
    private String uuid = "53E7B3B3-EBCF-46E3-BD28-9D6055EC7343";
    private String lty = "0";
    private String ab = "basic_js_pre=0, basic_android_webview=1, entername=1, basic_navi_icon=1, basic_cancelaccount_phonenumber=1, basic_intersoccer_list=2, videofeed_newtab_28=1, basic_newjr_redpoint=1, basic_navi_enter=1, basic_thread_pre=1, basic_image_load=1, basic_chinasoccer_list=2, interest_tag=0, kr_buffer_list_ab=1, gameenter=4, bbs_replylight_fake=1, bbs_waterfallsflow=0, bbs_hot24=0, clickad=1, basic_cancelaccount=1, newuserad=2, bbs_waterfallsflow_2=0, basic_recommond_interact=1, basic_recommond_interactdirect=1, basic_tab_lanuch=1, basic_upgradetoast=1, bbs_light_switch=1, soccer_h5_test=0, openad=1, basic_newuser_alert=2, gamereddot=2, searchad=2, basic_recommend_newjr=0, downad=1";


    private String dssid = "f686cfab-6a2d-40ac-b942-92cee9f1b8bf";
    private String ac = "hupuupdate";
    private String an = "hupu";
    private String av = "7.3.29.11681";
    private String blk = "BMC001";
    private String dname = "Weldon Chan的 iPhone";
    private String idfv = "53E7B3B3-EBCF-46E3-BD28-9D6055EC7343";
    private String net = "4g";
    private String osv = "13.1.3";
    private String pg = "PISC0002";
    private String pos = "T1";
    private String ssid = "BA358C066B7B1C92EC03DE8E1D070EE3";


    private String ext = "{\"label\":\"赛程\",\"pl\":\"fifa\"}";
    private String meta_pos = "179434439";
    private String meta_line_number = "78409";
    private String meta_file_name = "/data/hermes/access_log/8/access_log.2019112822";
    private String meta_host = "10.31.0.169";
    private String ua = "hupu/7.3.28.3 CFNetwork/1107.1 Darwin/19.0.0";
    private String st = "";
    private String cid = "33709426";
    private String bddid = "";
    private String act = "click";




//    PARTITIONED BY (ds string,act string)
//    LIFECYCLE 180;


}
