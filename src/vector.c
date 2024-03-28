#include <stdio.h>
#include <math.h>
#include <stdlib.h>

//test these functions using the test examples in the vectordb tutorial
//compare my computation with theirs

float compute_distance(float (*fcn)(float*, float*, int), float *v1, float *v2, int count) {
	return fcn(v1, v2, count);
}

float euclidean(float *v1, float *v2, int count) {
	float result = 0.0f;
	for (int i = 0; i < count; i++) {
		result += powf(*(v1 + i) - *(v2 + i), 2.0f);	
	}
	return sqrt(result);	
}

float inner_product(float *v1, float *v2, int count) {
	float result = 0.0f;
	for (int i = 0; i < count; i++) {
		result += *(v1 + i) * *(v2 + i);
	}
	return result;
}

float magnitude(float *v, int count) {
	float ssum = 0.0f;
	for (int i = 0; i < count; i++) {
		ssum += powf(*(v + i), 2.0f);
	}
	return sqrt(ssum);
}

float negative_inner_product(float *v1, float *v2, int count) {
	return -inner_product(v1, v2, count);
}

float cosine_similarity(float *v1, float *v2, int count) {
	return 1.0f - (inner_product(v1, v2, count) / (magnitude(v1, count) * magnitude(v2, count)));
}

struct vec3 {
	float values[3];
};

struct vec3_list {
	int count;
	struct vec3 *vectors;
};

struct vec3_list random_sample(struct vec3_list *data_to_index, int bucket_count) {
	struct vec3_list out_vl;
	out_vl.count = bucket_count;
	out_vl.vectors = malloc(sizeof(struct vec3) * out_vl.count);

	//choosing buckets as first and last vector in data for now
	out_vl.vectors[0] = data_to_index->vectors[0];
	int last_idx = data_to_index->count;
	out_vl.vectors[last_idx] = data_to_index->vectors[last_idx];

	return out_vl;
}

struct vec3_list find_centroids(struct vec3_list *data, struct vec3_list *prev_centroids) {
	struct vec3_list buckets[prev_centroids->count];
	//go through each data point and put into existing bucket from previous centroids
	//compute average of all points in new buckets, and use that as new centroids
	//return new buckets
}

int main() {
/*
	float vs[][4] = { { 1.0f, 1.0f, 1.0f },
				      { 2.0f, 1.0f, 1.0f },
				      { 3.0f, 1.0f, 1.0f },
				      { 4.0f, 1.0f, 1.0f } };

	float v[] = { 1.0f, 1.0f, 1.0f };

    printf("v euclidean vs:\n");
	for (int i = 0; i < 4; i++) {
		printf("%f\n", compute_distance(euclidean, v, vs[i], 3));
	}

    printf("v cosine_similarity vs:\n");
	for (int i = 0; i < 4; i++) {
		printf("%f\n", compute_distance(cosine_similarity, v, vs[i], 3));
	}

    printf("v inner product vs:\n");
	for (int i = 0; i < 4; i++) {
		printf("%f\n", compute_distance(negative_inner_product, v, vs[i], 3));
	}*/

	struct vec3 vs[4][3] = { { 1.0f, 1.0f, 1.0f },
				       		 { 2.0f, 1.0f, 1.0f },
				      		 { 3.0f, 1.0f, 1.0f },
				      		 { 4.0f, 1.0f, 1.0f } };
	struct vec3_list vectors;
	vectors.count = 4;
	vectors.vectors = (struct vec3*)vs;

	struct vec3_list centroids;
	centroids = random_sample(&vectors, 2);
	centroids = find_centroids(&vectors, &centroids);

	//centroid is a vector of the same dimensions as in the input vectors
	//centroids = random_sample(vectors, bucket_count);
	//for i in range(500):
	//	centroids = find_centroids(vectors, centroids);
	//return centroids;
}
