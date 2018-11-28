The increasing use of Web systems (e.g., digital libraries, social networks, and e-commerce) has become a valuable source of semi-structured data. In this context, the Entity Resolution task emerges as a fundamental step to integrate multiple knowledge bases or identify similarities between the data (i.e., entities). Usually, blocking techniques are widely applied as the initial step of Entity Resolution approaches in order to avoid computing the comparisons between all pairs of entities (quadratic cost). In practice, heterogeneous and noisy data increase the difficulties faced by blocking techniques. To address these challenges, we proposed the NA-BLOCKER technique, which is capable of tolerating noisy data to extract information regarding the schema and generate high-quality blocks (i.e., blocks that contain only entities with high chances of being considered correspondents). To this end, the NA-BLOCKER applies Locality Sensitive Hashing in order to hash the attribute values of the entities and enable the generation of high-quality blocks, even with the presence of noise in the attribute values. In this sense, since the Semantic Web approaches usually deal with noisy and heterogeneous data, the NA-BLOCKER technique can be useful for these approaches. Based on the experimental results, we can highlight that the NA-BLOCKER presents better results regarding effectiveness than the state-of-the-art technique, namely, BLAST.

To Run the project:

Main class> main/Test_metablockingNABLOCKER.java

Datasets> available at the project (dirty and clean)

Requered libs> This is a Maven project and there are additional libs at the "lib" path.
