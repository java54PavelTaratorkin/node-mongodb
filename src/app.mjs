import MongoConnection from "./mongo/MongoConnection.mjs";

const DB_NAME = 'sample_mflix';
const COLLECTION_MOVIES_NAME = "movies";
const COLLECTION_COMMENTS_NAME = "comments";
const mongoConnection = new MongoConnection(process.env.MONGO_URI, DB_NAME);
const collectionMovies = mongoConnection.getCollection(COLLECTION_MOVIES_NAME);
const collectionComments = mongoConnection.getCollection(COLLECTION_COMMENTS_NAME);

collectionMovies.aggregate([
    {
      '$bucketAuto': {
        'groupBy': '$imdb.rating', 
        'buckets': 5
      }
    }
  ]).toArray().then(data=>console.log("first",data));
collectionMovies.find({}).limit(1).project({'title':1,'_id':0}).toArray()
.then(data => console.log('second', data))

collectionComments.aggregate(
    [
      { $limit: 5 },
      {
        $lookup: {
          from: 'movies',
          localField: 'movie_id',
          foreignField: '_id',
          as: 'movieDetails'
        }
      },
      {
        $replaceRoot: {
          newRoot: {
            $mergeObjects: [
              '$$ROOT',
              {
                movie_id: {
                  $ifNull: [
                    { $arrayElemAt: ['$movieDetails.title', 0] },
                    'Unknown Title' // Fallback value if no match is found
                  ]
                }
              }
            ]
          }
        }
      },
      {
        $project: {
          _id: '$_id',
          name: '$name',
          email: '$email',
          title: '$movie_id',
          text: '$text',
          date: '$date'
        }
      }
    ],
    { maxTimeMS: 60000, allowDiskUse: true }
  )
    .toArray()
    .then(data => console.log("Task 1 Output:", data))
    .catch(err => console.error("Error in Task 1:", err));

  collectionMovies.aggregate(
  [
      {
          $facet: {
              averageRating: [
                  {
                      $group: {
                          _id: 0,
                          avgRating: { $avg: '$imdb.rating' }
                      }
                  }
              ],
              filteredMovies: [
                  {
                      $match: {
                          year: 2010,
                          genres: 'Comedy',
                          'imdb.rating': { $exists: true }
                      }
                  },
                  {
                      $project: {
                          title: 1,
                          'imdb.rating': 1
                      }
                  }
              ]
          }
      },
      {
          $project: {
              averageRating: {
                  $arrayElemAt: ['$averageRating.avgRating', 0]
              },
              filteredMovies: {
                  $filter: {
                      input: '$filteredMovies',
                      as: 'movie',
                      cond: {
                          $gt: [
                              '$$movie.imdb.rating',
                              {
                                  $arrayElemAt: ['$averageRating.avgRating', 0]
                              }
                          ]
                      }
                  }
              }
          }
      },
      {
          $project: {
              titles: '$filteredMovies.title'
          }
      }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
)
  .toArray()
  .then(data => console.log("Task 2 Output:", data[0].titles))
  .catch(err => console.error("Error in Task 2:", err));


  