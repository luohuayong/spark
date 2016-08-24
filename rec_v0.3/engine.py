from offline import user_rating
from offline import train_model
from offline import user_recommend
from offline import product_similarity
from offline import product_hot
from offline import user_cart_recommend

user_rating.run()
mod = train_model.run()
user_recommend.run(mod)
product_similarity.run(mod)
product_hot.run()
user_cart_recommend.run()

