#import pdb
#pdb.set_trace()
#import pudb;pu.db

'''
 items_similar [(pid,rpid,similarity)] rdd
 user_prefer [(uid,pid,rating)] rdd
'''
def recommend(items_similar,user_prefer):

    def map_fun1(f):
        i2_2 = sorted(f[1],reverse=True,key=lambda x:x[1])
        #if len(i2_2) > r_number :
        #    i2_2.remove(0,(len(i2_2)-r_number))
        return (f[0],i2_2)

    def map_fun2(f):
        r = []
        for w in f[1]:
            r.append((f[0],w[0],w[1]))
        return r

    #rdd_app1_R2 [(pid,((rpid,similarity),(uid,rating)))]
    rdd_app1_R2 = items_similar.map(lambda f:(f[0],(f[1],f[2])))\
        .join(user_prefer.map(lambda f:(f[1],(f[0],f[2]))))
    #rdd_app1_R3 [(uid,rpid),rating*similarity]
    rdd_app1_R3 = rdd_app1_R2.map(lambda f:((f[1][1][0],f[1][0][0]),f[1][1][1]*f[1][0][1]))
    #rdd_app1_R4 [(uid,rpid),sum(rating*similarity)]
    #rdd_app1_R4 [(uid,(rpid,sum(rating*similarity)))]
    rdd_app1_R4 = rdd_app1_R3.reduceByKey(lambda x,y:x+y)\
        .map(lambda f:(f[0][0],(f[0][1],f[1])))
    #rdd_app1_R5 [(uid,((rpid,sum(rating*similarity))...))]
    rdd_app1_R5 = rdd_app1_R4.groupByKey()
    #rdd_app1_R6 [(uid,sorted((rpid,sum(rating*similarity))...))]
    rdd_app1_R6 = rdd_app1_R5.map(lambda x:map_fun1(x))
    #rdd_app1_R7 [(uid,rpid,sum(rating*similarity))]
    rdd_app1_R7 = rdd_app1_R6.flatMap(lambda x:map_fun2(x))
#    rdd_app1_R7.map(lambda f:f[0]).distinct().count()
    return rdd_app1_R7
