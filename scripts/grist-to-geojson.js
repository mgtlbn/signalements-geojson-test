const https = require('https');
const fs = require('fs');

// Configuration
const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalements';

console.log('🚀 Démarrage de la fusion des 3 sources...\n');

// Fonction pour fetch HTTPS
function fetchUrl(url) {
    return new Promise((resolve, reject) => {
        https.get(url, (res) => {
            let data = '';
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`HTTP ${res.statusCode}`));
                } else {
                    try {
                        resolve(JSON.parse(data));
                    } catch (e) {
                        reject(new Error('JSON parse error'));
                    }
                }
            });
        }).on('error', reject);
    });
}

// Récupérer données Grist (API REST)
async function fetchGristData() {
    try {
        if (!GRIST_DOC_ID || !GRIST_API_KEY) {
            console.warn('⚠️  Grist credentials manquants, skipping...');
            return [];
        }

        console.log('🔗 [Grist 35] Récupération...');
        
        const options = {
            hostname: 'grist.dataregion.fr',
            path: `/o/inforoute/api/docs/${GRIST_DOC_ID}/tables/${TABLE_ID}/records`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${GRIST_API_KEY}`,
                'Content-Type': 'application/json'
            }
        };

        return new Promise((resolve, reject) => {
            https.get(options, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    if (res.statusCode === 200) {
                        try {
                            const parsed = JSON.parse(data);
                            console.log(`✅ [Grist 35] ${parsed.records.length} records`);
                            resolve(parsed.records || []);
                        } catch (e) {
                            console.error('❌ [Grist 35] Parse error');
                            resolve([]);
                        }
                    } else {
                        console.error(`❌ [Grist 35] HTTP ${res.statusCode}`);
                        resolve([]);
                    }
                });
            }).on('error', (err) => {
                console.error('❌ [Grist 35] Error:', err.message);
                resolve([]);
            });
        });
    } catch (error) {
        console.error('❌ [Grist 35] Error:', error.message);
        return [];
    }
}

// Récupérer données CD44
async function fetchCD44Data() {
    try {
        console.log('🔗 [CD44] Récupération...');
        const url = 'https://data.loire-atlantique.fr/api/explore/v2.1/catalog/datasets/224400028_info-route-departementale/records?limit=100';
        const response = await fetchUrl(url);
        const count = response.results ? response.results.length : 0;
        console.log(`✅ [CD44] ${count} records`);
        return response.results || [];
    } catch (error) {
        console.error('❌ [CD44] Error:', error.message);
        return [];
    }
}

// Récupérer données Rennes Métropole
async function fetchRennesMetropoleData() {
    try {
        console.log('🔗 [Rennes Métropole] Récupération...');
        
        const options = {
            hostname: 'data.rennesmetropole.fr',
            path: '/api/explore/v2.1/catalog/datasets/travaux_1_jour/records?limit=100',
            method: 'GET',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Connection': 'keep-alive'
            }
        };

        return new Promise((resolve, reject) => {
            https.get(options, (res) => {
                let data = '';
                res.on('data', chunk => { data += chunk; });
                res.on('end', () => {
                    if (res.statusCode !== 200) {
                        console.error(`❌ [Rennes Métropole] HTTP ${res.statusCode}`);
                        resolve([]);
                        return;
                    }
                    
                    try {
                        const response = JSON.parse(data);
                        const records = response.results || [];
                        console.log(`✅ [Rennes Métropole] ${records.length} records`);
                        resolve(records);
                    } catch (e) {
                        console.error('❌ [Rennes Métropole] Parse error');
                        resolve([]);
                    }
                });
            }).on('error', (err) => {
                console.error('❌ [Rennes Métropole] Error:', err.message);
                resolve([]);
            });
        });
    } catch (error) {
        console.error('❌ [Rennes Métropole] Error:', error.message);
        return [];
    }
}

// Convertir Grist record en GeoJSON feature
function gristToFeature(record) {
    try {
        let geometry;
        
        if (record.fields.geojson) {
            geometry = JSON.parse(record.fields.geojson);
        } else if (record.fields.Latitude && record.fields.Longitude) {
            geometry = {
                type: 'Point',
                coordinates: [record.fields.Longitude, record.fields.Latitude]
            };
        } else {
            return null;
        }
        
        const cause = Array.isArray(record.fields.Cause) ? 
                     record.fields.Cause.filter(c => c !== 'L').join(', ') : 
                     (record.fields.Cause || '');
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: record.id,
                source: 'Grist 35',
                gestionnaire: record.fields.Gestionnaire || record.fields.Administration || '',
                route: record.fields.Route || '',
                commune: record.fields.Commune || '',
                type_coupure: record.fields.Type_coupure || '',
                sens_circulation: record.fields.Sens_circulation || 'N/A',
                cause: cause,
                priorite: record.fields.Priorite || 'Moyenne',
                statut: record.fields.Statut || 'Actif',
                description: record.fields.Description || '',
                date_heure: record.fields.Date_heure || ''
            }
        };
    } catch (e) {
        console.warn(`⚠️  Erreur Grist record ${record.id}`);
        return null;
    }
}

// Convertir CD44 record en GeoJSON feature
function cd44ToFeature(item) {
    try {
        const geometry = {
            type: 'Point',
            coordinates: [item.longitude, item.latitude]
        };
        
        const route = Array.isArray(item.ligne2) ? item.ligne2.join(' / ') : (item.ligne2 || 'Route');
        const commune = item.ligne3 || 'Commune';
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `cd44-${item.recordid}`,
                source: 'CD44',
                gestionnaire: 'CD44',
                route: route,
                commune: commune,
                type_coupure: item.type || '',
                cause: item.nature || '',
                priorite: 'Moyenne',
                statut: 'Actif',
                description: item.ligne1 || '',
                date_heure: item.datepublication || ''
            }
        };
    } catch (e) {
        console.warn(`⚠️  Erreur CD44 record`);
        return null;
    }
}

// Convertir Rennes Métropole record en GeoJSON feature
function rennesMetropoleToFeature(item) {
    try {
        let geometry = null;
        
        // Essayer geo_shape d'abord
        if (item.geo_shape && item.geo_shape.geometry) {
            const geom = item.geo_shape.geometry;
            if (geom.type === 'Point') {
                geometry = {
                    type: 'Point',
                    coordinates: geom.coordinates
                };
            } else if (geom.type === 'LineString') {
                geometry = {
                    type: 'LineString',
                    coordinates: geom.coordinates
                };
            }
        }
        // Fallback sur geo_point_2d
        else if (item.geo_point_2d) {
            geometry = {
                type: 'Point',
                coordinates: [item.geo_point_2d.lon, item.geo_point_2d.lat]
            };
        }
        
        if (!geometry) return null;
        
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `rm-${item.recordid}`,
                source: 'Rennes Métropole',
                gestionnaire: 'Rennes Métropole',
                route: item.localisation || item.rue || '',
                commune: item.commune || 'Rennes',
                cause: 'Travaux',
                priorite: 'Moyenne',
                statut: 'Actif',
                description: item.libelle || '',
                date_heure: item.date_deb || ''
            }
        };
    } catch (e) {
        console.warn(`⚠️  Erreur Rennes Métropole record`);
        return null;
    }
}

// Fusion principale
async function mergeSources() {
    try {
        console.log('');
        
        // Récupérer les 3 sources en parallèle
        const [gristRecords, cd44Records, rennesMetropoleRecords] = await Promise.all([
            fetchGristData(),
            fetchCD44Data(),
            fetchRennesMetropoleData()
        ]);
        
        console.log(`\n📊 Total brut: ${gristRecords.length + cd44Records.length + rennesMetropoleRecords.length} records\n`);
        
        // Convertir en features
        let features = [];
        
        // Grist
        gristRecords.forEach(record => {
            const feature = gristToFeature(record);
            if (feature) features.push(feature);
        });
        
        // CD44
        cd44Records.forEach(item => {
            const feature = cd44ToFeature(item);
            if (feature) features.push(feature);
        });
        
        // Rennes Métropole
        rennesMetropoleRecords.forEach(item => {
            const feature = rennesMetropoleToFeature(item);
            if (feature) features.push(feature);
        });
        
        console.log(`✅ ${features.length} features créées\n`);
        
        // Créer GeoJSON
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Fusion Grist 35 + CD44 + Rennes Métropole',
                total_count: features.length,
                sources: {
                    grist_35: gristRecords.length,
                    cd44: cd44Records.length,
                    rennes_metropole: rennesMetropoleRecords.length
                }
            }
        };
        
        // Écrire le fichier
        fs.writeFileSync('signalements.geojson', JSON.stringify(geojson, null, 2));
        console.log('✅ Fichier signalements.geojson créé');
        
        // Metadata
        const metadata = {
            lastUpdate: new Date().toISOString(),
            sources: {
                grist_35: gristRecords.length,
                cd44: cd44Records.length,
                rennes_metropole: rennesMetropoleRecords.length,
                total: features.length
            },
            stats: {
                points: features.filter(f => f.geometry.type === 'Point').length,
                lines: features.filter(f => f.geometry.type === 'LineString').length,
                by_source: {
                    grist_35: features.filter(f => f.properties.source === 'Grist 35').length,
                    cd44: features.filter(f => f.properties.source === 'CD44').length,
                    rennes_metropole: features.filter(f => f.properties.source === 'Rennes Métropole').length
                }
            }
        };
        
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Métadonnées créées');
        
        console.log('\n📊 Statistiques finales:');
        console.log(`   - Grist 35: ${gristRecords.length}`);
        console.log(`   - CD44: ${cd44Records.length}`);
        console.log(`   - Rennes Métropole: ${rennesMetropoleRecords.length}`);
        console.log(`   - Total: ${features.length}`);
        console.log(`   - Points: ${metadata.stats.points}`);
        console.log(`   - Lignes: ${metadata.stats.lines}`);
        
    } catch (error) {
        console.error('❌ Erreur fusion:', error.message);
        process.exit(1);
    }
}

// Lancer
mergeSources();

// Conversion en GeoJSON avec fusion de 3 sources
async function convertToGeoJSON() {
    try {
        console.log('🚀 Démarrage de la fusion des 3 sources...\n');
        
        // Récupérer toutes les sources en parallèle (comme Promise.all en HTML)
        const allData = await Promise.all(
            GRIST_SOURCES.map(source => fetchGristData(source))
        );
        
        // Fusionner tous les records de toutes les sources
        const allRecords = allData.flatMap(data => data.records || []);
        console.log(`\n✅ Total: ${allRecords.length} enregistrements récupérés`);
        
        // Construire le GeoJSON à partir de tous les records fusionnés
        const features = allRecords
            .filter(record => {
                return record.fields.geojson || 
                       (record.fields.Latitude && record.fields.Longitude);
            })
            .map(record => {
                try {
                    let geometry;
                    
                    // Format GeoJSON
                    if (record.fields.geojson) {
                        geometry = JSON.parse(record.fields.geojson);
                    }
                    // Format Latitude/Longitude
                    else if (record.fields.Latitude && record.fields.Longitude) {
                        // Ligne (tronçon)
                        if (record.fields.Latitude_fin && record.fields.Longitude_fin) {
                            geometry = {
                                type: 'LineString',
                                coordinates: [
                                    [record.fields.Longitude, record.fields.Latitude],
                                    [record.fields.Longitude_fin, record.fields.Latitude_fin]
                                ]
                            };
                        }
                        // Point
                        else {
                            geometry = {
                                type: 'Point',
                                coordinates: [record.fields.Longitude, record.fields.Latitude]
                            };
                        }
                    }
                    
                    return {
                        type: 'Feature',
                        geometry: geometry,
                        properties: {
                            id: record.id,
                            administration: record.fields.Administration || record.fields.Agent || 'Non spécifié',
                            route: record.fields.Route || '',
                            commune: record.fields.Commune || '',
                            type_coupure: record.fields.Type_coupure || '',
                            sens_circulation: record.fields.Sens_circulation || 'N/A',
                            cause: Array.isArray(record.fields.Cause) ? 
                                   record.fields.Cause.join(', ') : 
                                   (record.fields.Cause || ''),
                            priorite: record.fields.Priorite || 'Moyenne',
                            statut: record.fields.Statut || 'Actif',
                            description: record.fields.Description || '',
                            date_heure: record.fields.Date_heure || '',
                            geometrie_type: record.fields.geometrie_type || geometry.type
                        }
                    };
                } catch (e) {
                    console.warn(`⚠️ Erreur pour l'enregistrement ${record.id}:`, e.message);
                    return null;
                }
            })
            .filter(f => f !== null);
        
        const geojson = {
            type: 'FeatureCollection',
            features: features,
            metadata: {
                generated: new Date().toISOString(),
                source: 'Grist - Signalements routiers Ille-et-Vilaine',
                count: features.length,
                doc_id: GRIST_DOC_ID,
                table: TABLE_ID
            }
        };
        
        console.log(`✅ ${features.length} features créées`);
        
        // Écrire le fichier GeoJSON
        fs.writeFileSync('signalements.geojson', JSON.stringify(geojson, null, 2));
        console.log('✅ Fichier signalements.geojson créé avec succès !');
        
        // Créer métadonnées
        const metadata = {
            lastUpdate: new Date().toISOString(),
            recordCount: features.length,
            pointCount: features.filter(f => f.geometry.type === 'Point').length,
            lineCount: features.filter(f => f.geometry.type === 'LineString').length,
            polygonCount: features.filter(f => f.geometry.type === 'Polygon').length,
            priorites: {
                critique: features.filter(f => f.properties.priorite === 'Critique').length,
                haute: features.filter(f => f.properties.priorite === 'Haute').length,
                moyenne: features.filter(f => f.properties.priorite === 'Moyenne').length,
                basse: features.filter(f => f.properties.priorite === 'Basse').length
            },
            statuts: {
                actif: features.filter(f => f.properties.statut === 'Actif').length,
                resolu: features.filter(f => f.properties.statut === 'Resolu').length
            }
        };
        
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Métadonnées créées');
        console.log('\n📊 Statistiques GLOBALES:');
        console.log(`   - Total: ${metadata.recordCount}`);
        console.log(`   - Points: ${metadata.pointCount}`);
        console.log(`   - Lignes: ${metadata.lineCount}`);
        console.log(`   - Polygones: ${metadata.polygonCount}`);
        console.log(`   - Actifs: ${metadata.statuts.actif}`);
        console.log(`   - Résolus: ${metadata.statuts.resolu}`);
        
    } catch (error) {
        console.error('❌ Erreur:', error.message);
        console.error('Stack:', error.stack);
        process.exit(1);
    }
}

// Lancer la conversion
convertToGeoJSON();
