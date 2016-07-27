# -*- coding:utf-8 -*-
from flask import Blueprint
main = Blueprint('main',__name__)

import json
from engine import RecommendationEngine

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask,request

@main.route("/<int:user_id>/ratings/top/<int:count>",methods=["GET"])
def top_ratings(user_id,count):
    logger.debug("User %s top ratings requested",user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)

@main.route("/<int:user_id>/ratings/<int:goods_id>",methods=["GET"])
def goods_rating(user_id,goods_id):
    logger.debug("User %s to goods %s rating requested",user_id,goods_id)
    rating = recommendation_engine.get_rating_for_goods(user_id,goods_id)
    return json.dumps(rating)

@main.route("/<int:user_id>/ratings",methods=["POST"])
def add_ratings(user_id):
    ratings_list = request.form.keys()[0].strip().split("\n")
    ratings_list = map(lambda x:x.split(","),ratings_list)
    #create a list with the format by engine (user_id,googds_id,rating)
    ratings = map(lambda x:(user_id,int(x[0]),float(x[1])),rating_list)
    recommendation_engine.add_ratings(ratings)
    return json.dumps(ratings)

def create_app(spark_context,dataset_path):
    global recommendation_engine
    recommendation_engine = RecommendationEngine(spark_context,dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app

