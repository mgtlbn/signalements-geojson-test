const https = require('https');
const fs = require('fs');

// Configuration depuis les variables d'environnement
const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_ID = 'Signalements'; // ⚠️ CHANGÉ : "Signalements" au lieu de "Signalement"

if (!GRIST_DOC_ID || !GRIST_API_KEY) {
    console.error('❌ Variables d\'environnement manquantes');
    console.error('GRIST_DOC_ID:', GRIST_DOC_ID ? '✓' : '✗');
    console.error('GRIST_API_KEY:', GRIST_API_KEY ? '✓' : '✗');
    process.exit(1);
}

// Fonction pour récupérer les données Grist
function fetchGristData() {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'grist.numerique.gouv.fr', // ⚠️ CHANGÉ : votre instance Grist
            path: `/api/docs/${GRIST_DOC_ID}/tables/${TABLE_ID}/records`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${GRIST_API_KEY}`,
                'Content-Type': 'application/json'
            }
        };

        https.get(options, (res) => {
            let data = '';
            
            res.on('data', (chunk) => {
                data += chunk;
            });
            
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    reject(new Error(`Erreur API: ${res.statusCode} - ${data}`));
                } else {
                    try {
                        resolve(JSON.parse(data));
                    } catch (e) {
                        reject(new Error('Erreur parsing JSON: ' + e.message));
                    }
                }
            });
        }).on('error', (err) => {
            reject(err);
        });
    });
}

// Conversion en GeoJSON
async function convertToGeoJSON() {
    try {
        console.log('🔄 Récupération des données depuis Grist...');
        const data = await fetchGristData();
        
        console.log(`✅ ${data.records.length} enregistrements récupérés`);
        
        // Construire le GeoJSON avec les VRAIS noms de colonnes
        const features = data.records
            .filter(record => {
                // Filtrer les enregistrements avec geojson OU avec Latitude/Longitude
                return record.fields.geojson || 
                       (record.fields.Latitude && record.fields.Longitude);
            })
            .map(record => {
                try {
                    let geometry;
                    
                    // NOUVEAU FORMAT : geojson existe
                    if (record.fields.geojson) {
                        geometry = JSON.parse(record.fields.geojson);
                    }
                    // ANCIEN FORMAT : Latitude/Longitude
                    else if (record.fields.Latitude && record.fields.Longitude) {
                        // Si c'est un tronçon (ligne)
                        if (record.fields.Latitude_fin && record.fields.Longitude_fin) {
                            geometry = {
                                type: 'LineString',
                                coordinates: [
                                    [record.fields.Longitude, record.fields.Latitude],
                                    [record.fields.Longitude_fin, record.fields.Latitude_fin]
                                ]
                            };
                        }
                        // Sinon c'est un point
                        else {
                            geometry = {
                                type: 'Point',
                                coordinates: [record.fields.Longitude, record.fields.Latitude]
                            };
                        }
                    }
                    
                    // Construire les propriétés avec les VRAIS noms de colonnes
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
        
        // Créer un fichier de métadonnées détaillé
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
        console.log('\n📊 Statistiques:');
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
