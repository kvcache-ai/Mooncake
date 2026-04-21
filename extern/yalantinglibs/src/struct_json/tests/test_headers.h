#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

#include "ylt/reflection/user_reflect_macro.hpp"

struct MyClass1 {
  double member0;
  double member1;
  double member2;
  double member3;

  bool operator==(MyClass1 const &rhs) const {
    return member0 == rhs.member0 && member1 == rhs.member1 &&
           member2 == rhs.member2 && member3 == rhs.member3;
  }
};
YLT_REFL(MyClass1, member0, member1, member2, member3);

struct MyClass2 {
  unsigned member_unsigned0;
  unsigned member_unsigned1;
  signed member_signed;

  bool operator==(MyClass2 const &rhs) const {
    return member_unsigned0 == rhs.member_unsigned0 &&
           member_unsigned1 == rhs.member_unsigned1 &&
           member_signed == rhs.member_signed;
  }
};
YLT_REFL(MyClass2, member_unsigned0, member_unsigned1, member_signed);

struct person {
  std::string name;
  int age;
};
YLT_REFL(person, name, age);

// canada.json
struct property_t {
  std::string name;
};  // Property
YLT_REFL(property_t, name);

struct polygon_t {
  std::string type;
  std::vector<std::vector<std::array<double, 2>>> coordinates;
};  // Polygon
YLT_REFL(polygon_t, type, coordinates);

struct feature_t {
  std::string type;
  property_t properties;
  polygon_t geometry;
};  // Feature
YLT_REFL(feature_t, type, properties, geometry);

struct FeatureCollection {
  std::string type;
  std::vector<feature_t> features;
};  // FeatureCollection
YLT_REFL(FeatureCollection, type, features);

// apache_builds.json
struct jobs_t {
  std::string name;
  std::string url;
  std::string color;
};
YLT_REFL(jobs_t, name, url, color);

struct views_t {
  std::string name;
  std::string url;
};
YLT_REFL(views_t, name, url);

struct apache_empty_t {};
YLT_REFL(apache_empty_t);

struct apache_builds {
  std::vector<apache_empty_t> assignedLabels;
  std::string mode;
  std::string nodeDescription;
  std::string nodeName;
  int64_t numExecutors;
  std::string description;
  std::vector<jobs_t> jobs;
  apache_empty_t overallLoad;
  views_t primaryView;
  bool quietingDown;
  int64_t slaveAgentPort;
  apache_empty_t unlabeledLoad;
  bool useCrumbs;
  bool useSecurity;
  std::vector<views_t> views;
};
YLT_REFL(apache_builds, assignedLabels, mode, nodeDescription, nodeName,
         numExecutors, description, jobs, overallLoad, primaryView,
         quietingDown, slaveAgentPort, unlabeledLoad, useCrumbs, useSecurity,
         views);

// citm_catalog.json
struct events_value_t {
  std::optional<std::string> description;
  std::int64_t id;
  std::optional<std::string> logo;
  std::string name;
  std::vector<std::int64_t> subTopicIds;
  std::optional<int64_t> subjectCode;
  std::optional<std::string> subtitle;
  std::vector<std::int64_t> topicIds;
};  // events_value_t
YLT_REFL(events_value_t, description, id, logo, name, subTopicIds, subjectCode,
         subtitle, topicIds);

struct prices_element_t {
  std::int64_t amount;
  std::int64_t audienceSubCategoryId;
  std::int64_t seatCategoryId;
};  // prices_element_t
YLT_REFL(prices_element_t, amount, audienceSubCategoryId, seatCategoryId);

struct areas_element_t {
  std::int64_t areaId;
  std::vector<int64_t> blockIds;
};  // areas_element_t
YLT_REFL(areas_element_t, areaId, blockIds);

struct seatCategories_element_t {
  std::vector<areas_element_t> areas;
  std::int64_t seatCategoryId;
};  // seatCategories_element_t
YLT_REFL(seatCategories_element_t, areas, seatCategoryId);

struct performances_element_t {
  std::int64_t eventId;
  std::int64_t id;
  std::optional<std::string> logo;
  std::optional<std::string> name;
  std::vector<prices_element_t> prices;
  std::vector<seatCategories_element_t> seatCategories;
  std::optional<std::string> seatMapImage;
  std::int64_t start;
  std::string venueCode;
};  // performances_element_t
YLT_REFL(performances_element_t, eventId, id, logo, name, prices,
         seatCategories, seatMapImage, start, venueCode);

struct venueNames_t {
  std::string PLEYEL_PLEYEL;
};  // venueNames_t
YLT_REFL(venueNames_t, PLEYEL_PLEYEL);

struct citm_object_t {
  std::unordered_map<std::int64_t, std::string> areaNames;
  std::unordered_map<std::int64_t, std::string> audienceSubCategoryNames;
  apache_empty_t blockNames;
  std::unordered_map<std::int64_t, events_value_t> events;
  std::vector<performances_element_t> performances;
  std::unordered_map<std::string, std::string> seatCategoryNames;
  std::unordered_map<std::string, std::string> subTopicNames;
  apache_empty_t subjectNames;
  std::unordered_map<std::string, std::string> topicNames;
  std::unordered_map<std::string, std::vector<std::int64_t>> topicSubTopics;
  std::optional<venueNames_t> venueNames;
};  // citm_object_t
YLT_REFL(citm_object_t, areaNames, audienceSubCategoryNames, blockNames, events,
         performances, seatCategoryNames, subTopicNames, subjectNames,
         topicNames, topicSubTopics, venueNames);

// gsoc-2018.json
struct sponsor_t {
  std::string type;  //@
  std::string name;
  std::string disambiguatingDescription;
  std::string description;
  std::string url;
  std::string logo;
};
YLT_REFL(sponsor_t, type, name, disambiguatingDescription, description, url,
         logo);

struct author_t {
  std::string type;  //@
  std::string name;
};
YLT_REFL(author_t, type, name);

struct gsoc_element_t {
  std::string context;  //@
  std::string type;     //@
  std::string name;
  std::string description;
  sponsor_t sponsor;
  author_t author;
};
YLT_REFL(gsoc_element_t, context, type, name, description, sponsor, author);

using gsoc_object_t = std::map<int, gsoc_element_t>;

// mesh.pretty.json
struct mesh_element_t {
  std::vector<int> indexRange;
  std::vector<int> usedBones;
  std::vector<int> vertexRange;
};
YLT_REFL(mesh_element_t, indexRange, usedBones, vertexRange);

struct mesh_t {
  std::vector<mesh_element_t> batches;
  std::vector<int64_t> colors;
  std::vector<int> indices;
  std::vector<std::vector<double>> influences;
  apache_empty_t morphTargets;
  std::vector<double> normals;
  std::vector<double> positions;
  std::vector<double> tex0;
};
YLT_REFL(mesh_t, batches, colors, indices, influences, morphTargets, normals,
         positions, tex0);

// random.json
struct friend_t {
  int id;
  std::string name;
  std::string phone;
};
YLT_REFL(friend_t, id, name, phone);

struct random_element_t {
  int id;
  std::string avatar;
  int age;
  bool admin;
  std::string name;
  std::string company;
  std::string phone;
  std::string email;
  std::string birthDate;
  std::vector<friend_t> friends;
  std::string field;
};
YLT_REFL(random_element_t, id, avatar, age, admin, name, company, phone, email,
         birthDate, friends, field);

struct random_t {
  int id;
  std::string jsonrpc;
  int total;
  std::vector<random_element_t> result;
};
YLT_REFL(random_t, id, jsonrpc, total, result);

// github_events.json
namespace githubEvents {
struct user_t {
  std::string gists_url;
  std::string gravatar_id;
  std::string url;
  std::string type;
  std::string avatar_url;
  std::string subscriptions_url;
  std::string organizations_url;
  std::string received_events_url;
  std::string repos_url;
  std::string login;
  std::string starred_url;
  int id;
  std::string events_url;
  std::string followers_url;
  std::string following_url;
};
YLT_REFL(user_t, gists_url, gravatar_id, url, type, avatar_url,
         subscriptions_url, organizations_url, received_events_url, repos_url,
         login, starred_url, id, events_url, followers_url, following_url);

struct page_t {
  std::string page_name;
  std::string html_url;
  std::string title;
  std::string sha;
  std::optional<std::string> summary;
  std::string action;
};
YLT_REFL(page_t, page_name, html_url, title, sha, summary, action);

struct pull_request_t {
  std::optional<std::string> html_url;
  std::optional<std::string> patch_url;
  std::optional<std::string> diff_url;
};
YLT_REFL(pull_request_t, html_url, patch_url, diff_url);

struct forkee_t {
  std::string full_name;
  std::string stargazers_url;
  std::string clone_url;
  bool fork;
  std::string url;
  std::string tags_url;
  std::string description;
  std::string merges_url;
  int forks;
  std::string language;
  bool ___private;
  std::string archive_url;
  std::string collaborators_url;
  std::string languages_url;
  user_t owner;
  std::string git_refs_url;
  std::string labels_url;
  std::string pushed_at;
  std::string html_url;
  std::string trees_url;
  std::string forks_url;
  std::string commits_url;
  std::string branches_url;
  std::string notifications_url;
  std::string created_at;
  bool has_issues;
  std::string blobs_url;
  std::string issues_url;
  std::string compare_url;
  int open_issues;
  std::string contents_url;
  std::string name;
  std::string statuses_url;
  std::string assignees_url;
  int forks_count;
  std::string updated_at;
  std::string issue_events_url;
  std::string ssh_url;
  std::string subscribers_url;
  std::optional<std::string> mirror_url;
  bool ___public;
  bool has_wiki;
  std::string git_commits_url;
  std::string downloads_url;
  int id;
  std::string pulls_url;
  bool has_downloads;
  std::string issue_comment_url;
  int watchers_count;
  std::optional<std::string> homepage;
  std::string hooks_url;
  std::string subscription_url;
  std::string milestones_url;
  std::string events_url;
  std::string svn_url;
  std::string git_tags_url;
  std::string teams_url;
  std::string comments_url;
  int open_issues_count;
  std::string keys_url;
  std::string contributors_url;
  int size;
  int watchers;
  std::string git_url;
};
YLT_REFL(forkee_t, full_name, stargazers_url, clone_url, fork, url, tags_url,
         description, merges_url, forks, language, ___private, archive_url,
         collaborators_url, languages_url, owner, git_refs_url, labels_url,
         pushed_at, html_url, trees_url, forks_url, commits_url, branches_url,
         notifications_url, created_at, has_issues, blobs_url, issues_url,
         open_issues, contents_url, name, statuses_url, assignees_url,
         forks_count, updated_at, issue_events_url, ssh_url, subscribers_url,
         mirror_url, ___public, has_wiki, git_commits_url, downloads_url, id,
         pulls_url, has_downloads, issue_comment_url, watchers_count, homepage,
         hooks_url, subscription_url, milestones_url, events_url, svn_url,
         git_tags_url, teams_url, comments_url, open_issues_count, keys_url,
         contributors_url, size, watchers, git_url, compare_url);

struct issue_t {
  user_t user;
  std::string url;
  std::vector<std::string> labels;
  std::string html_url;
  std::string labels_url;
  pull_request_t pull_request;
  std::string created_at;
  std::optional<std::string> closed_at;
  std::optional<std::string> milestone;
  std::string title;
  std::string body;
  std::string updated_at;
  int number;
  std::string state;
  std::optional<user_t> assignee;
  int id;
  int comments;
  std::string events_url;
  std::string comments_url;
};
YLT_REFL(issue_t, user, url, labels, html_url, labels_url, pull_request,
         created_at, closed_at, milestone, title, body, updated_at, number,
         state, assignee, id, comments, events_url, comments_url);

struct comment_t {
  user_t user;
  std::string url;
  std::string issue_url;
  std::string created_at;
  std::string body;
  std::string updated_at;
  int id;
};
YLT_REFL(comment_t, user, url, issue_url, created_at, body, updated_at, id);

struct actor_org_t {
  std::string gravatar_id;
  std::string login;
  std::string avatar_url;
  std::string url;
  int id;
};
YLT_REFL(actor_org_t, gravatar_id, login, avatar_url, url, id);

struct repo_t {
  std::string url;
  int id;
  std::string name;
};
YLT_REFL(repo_t, url, id, name);

struct author_t {
  std::string email;
  std::string name;
};
YLT_REFL(author_t, email, name);

struct commit_t {
  std::string url;
  std::string message;
  bool distinct;
  std::string sha;
  author_t author;
};
YLT_REFL(commit_t, url, message, distinct, sha, author);

struct payload_t {
  std::optional<std::vector<commit_t>> commits;
  std::optional<int> distinct_size;
  std::optional<std::string> ref;
  std::optional<int> push_id;
  std::optional<std::string> head;
  std::optional<std::string> before;
  std::optional<int> size;
  std::optional<forkee_t> forkee;
  std::optional<std::vector<page_t>> pages;
  std::optional<std::string> action;
  std::optional<comment_t> comment;
  std::optional<issue_t> issue;
  std::optional<std::string> description;
  std::optional<std::string> master_branch;

  std::optional<std::string> ref_type;
};
YLT_REFL(payload_t, commits, distinct_size, ref, push_id, head, before, size,
         forkee, pages, action, comment, issue, description, master_branch,
         ref_type);

struct event_t {
  std::string type;
  std::string created_at;
  std::optional<actor_org_t> actor;
  repo_t repo;
  bool ___public;
  std::optional<actor_org_t> org;
  payload_t payload;
  std::string id;
};
YLT_REFL(event_t, type, created_at, actor, repo, ___public, org, payload, id);

using events_t = std::vector<event_t>;
}  // namespace githubEvents

namespace marine_ik {
struct image_element_t {
  std::string url;
  std::string uuid;
  std::string name;
};
YLT_REFL(image_element_t, url, uuid, name);

struct item_t {
  std::string name;
  std::string type;
  std::string uuid;
};
YLT_REFL(item_t, name, type, uuid);

struct key_element_t {
  std::array<float, 4> rot;
  float time{};
  std::array<float, 3> scl;
  std::array<float, 3> pos;
};
YLT_REFL(key_element_t, rot, time, scl, pos);

struct hierarchy_element_t {
  int parent;
  std::vector<key_element_t> keys;
};
YLT_REFL(hierarchy_element_t, parent, keys);

struct geo_anim_element_t {
  std::vector<hierarchy_element_t> hierarchy;
  float length{};
  int fps{};
  std::string name;
};
YLT_REFL(geo_anim_element_t, hierarchy, length, fps, name);

struct bone_element_t {
  int parent;
  std::array<float, 3> pos;
  std::array<float, 4> rotq;
  std::array<int, 3> scl;
  std::string name;
};
YLT_REFL(bone_element_t, parent, pos, rotq, scl, name);

struct geo_meta_data_t {
  int uvs;
  int version;
  int faces;
  std::string generator;
  int normals;
  int bones;
  int vertices;
};
YLT_REFL(geo_meta_data_t, uvs, version, faces, generator, normals, bones,
         vertices);

struct geo_data_t {
  std::vector<std::vector<float>> uvs;
  std::vector<geo_anim_element_t> animations;
  std::vector<float> vertices;
  geo_meta_data_t metadata;
  std::string name;
  std::vector<float> skinWeights;
  std::vector<int> skinIndices;
  int influencesPerVertex{};
  std::vector<float> normals;
  std::vector<bone_element_t> bones;
  std::vector<int> faces;
};
YLT_REFL(geo_data_t, uvs, animations, vertices, metadata, name, skinWeights,
         skinIndices, influencesPerVertex, normals, bones, faces);

struct geometry_element_t {
  std::string type;
  std::string uuid;
  geo_data_t data;
};
YLT_REFL(geometry_element_t, type, uuid, data);

struct texture_element_t {
  std::array<int, 2> repeat;
  std::array<int, 2> wrap;
  int anisotropy{};
  std::string image;
  std::string name;
  int mapping{};
  int minFilter{};
  std::string uuid;
  int magFilter{};
};
YLT_REFL(texture_element_t, repeat, wrap, anisotropy, image, name, mapping,
         minFilter, uuid, magFilter);

struct meta_data_t {
  std::string sourceFile;
  std::string generator;
  std::string type;
  float version{};
};
YLT_REFL(meta_data_t, sourceFile, generator, type, version);

struct material_element_t : item_t {
  int vertexColors{};
  std::string blending;
  std::string map;
  bool transparent{};
  bool depthTest{};
  int color;
  int shininess;
  int emissive;
  bool depthWrite{};
  int specular{};
};
YLT_REFL(material_element_t, vertexColors, name, type, uuid, blending, map,
         transparent, depthTest, color, shininess, emissive, depthWrite,
         specular);

struct obj_child_t : item_t {
  std::array<float, 16> matrix;
  bool visible{};
  std::string material;
  bool castShadow{};
  bool receiveShadow{};
  std::string geometry;
};
YLT_REFL(obj_child_t, name, uuid, matrix, visible, type, material, castShadow,
         receiveShadow, geometry);

struct object_t {
  std::vector<obj_child_t> children;
  std::string type;
  std::array<float, 16> matrix;
  std::string uuid;
};
YLT_REFL(object_t, children, type, matrix, uuid);

struct animation_element_t {
  std::vector<int> tracks;
  int fps;
  std::string name;
};
YLT_REFL(animation_element_t, tracks, fps, name);

struct marine_ik_t {
  std::vector<image_element_t> images;
  std::vector<geometry_element_t> geometries;
  std::vector<texture_element_t> textures;
  meta_data_t metadata;
  std::vector<material_element_t> materials;
  object_t object;
  std::vector<animation_element_t> animations;
};
YLT_REFL(marine_ik_t, images, geometries, textures, metadata, materials, object,
         animations);
}  // namespace marine_ik

// instruments.json
struct sample_element {
  int c5_samplerate;
  int global_volume;
  std::string legacy_filename;
  int length;
  int loop_end;
  int loop_start;
  std::string name;
  int pan;
  int sustain_end;
  int sustain_start;
  int vibrato_depth;
  int vibrato_rate;
  int vibrato_sweep;
  int vibrato_type;
  int volume;
};
YLT_REFL(sample_element, c5_samplerate, global_volume, legacy_filename, length,
         loop_end, loop_start, name, pan, sustain_end, sustain_start,
         vibrato_depth, vibrato_rate, vibrato_sweep, vibrato_type, volume);

struct data_t {
  int channel;
  int fxcmd;
  int fxparam;
  int instr;
  int note;
  int row;
  int volcmd;
  int volval;
};
YLT_REFL(data_t, channel, fxcmd, fxparam, instr, note, row, volcmd, volval);

struct pattern_element {
  std::optional<std::vector<data_t>> data;
  std::string name;
  int rows;
  int rows_per_beat;
  int rows_per_measure;
};
YLT_REFL(pattern_element, data, name, rows, rows_per_beat, rows_per_measure);

struct node_t {
  int tick;
  int value;
};
YLT_REFL(node_t, tick, value);

struct panning_envelope_t {
  int loop_end;
  int loop_start;
  std::vector<node_t> nodes;
  int release_node;
  int sustain_end;
  int sustain_start;
};
YLT_REFL(panning_envelope_t, loop_end, loop_start, nodes, release_node,
         sustain_end, sustain_start);

struct instrument_element {
  int default_filter_cutoff;
  bool default_filter_cutoff_enabled;
  int default_filter_mode;
  int default_filter_resonance;
  bool default_filter_resonance_enabled;
  int default_pan;
  int duplicate_check_type;
  int duplicate_note_action;
  int fadeout;
  int global_volume;
  int graph_insert;
  std::string legacy_filename;
  int midi_bank;
  int midi_channel;
  int midi_drum_set;
  int midi_program;
  std::string name;
  int new_note_action;
  std::optional<int> note_map;
  panning_envelope_t panning_envelope;
  panning_envelope_t pitch_envelope;

  int pitch_pan_center;
  int pitch_pan_separation;
  int pitch_to_tempo_lock;
  int random_cutoff_weight;
  int random_pan_weight;
  int random_resonance_weight;
  int random_volume_weight;
  std::optional<int> sample_map;
  std::optional<int> tuning;

  panning_envelope_t volume_envelope;
  int volume_ramp_down;
  int volume_ramp_up;
};
YLT_REFL(instrument_element, default_filter_cutoff,
         default_filter_cutoff_enabled, default_filter_mode,
         default_filter_resonance, default_filter_resonance_enabled,
         default_pan, duplicate_check_type, duplicate_note_action, fadeout,
         global_volume, graph_insert, legacy_filename, midi_bank, midi_channel,
         midi_drum_set, midi_program, name, new_note_action, note_map,
         panning_envelope, pitch_envelope, pitch_pan_center,
         pitch_pan_separation, pitch_to_tempo_lock, random_cutoff_weight,
         random_pan_weight, random_resonance_weight, random_volume_weight,
         sample_map, tuning, volume_envelope, volume_ramp_down, volume_ramp_up);

struct instruments_t {
  std::optional<int> graphstate;
  std::vector<instrument_element> instruments;
  std::optional<std::string> message;
  std::string name;
  std::optional<std::string> orderlist;
  std::vector<pattern_element> patterns;
  std::optional<int> pluginstate;
  std::vector<sample_element> samples;
  int version;
};
YLT_REFL(instruments_t, graphstate, instruments, message, name, orderlist,
         patterns, pluginstate, samples, version);
